package ravenmq_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"github.com/kmr-ankitt/ravenmq/internal/broker"
	"github.com/kmr-ankitt/ravenmq/internal/coordinator"
	"github.com/kmr-ankitt/ravenmq/internal/groupcoord"
	"github.com/kmr-ankitt/ravenmq/pkg/ravenmq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
freeTCPAddr allocates an available local TCP port and returns its host:port string.

steps:
1. open a temporary TCP listener on 127.0.0.1:0.
2. capture the assigned local address and immediately close the listener.

Returns:
- an available local TCP address string.
*/
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

/*
startTestCoordinator boots a single-node Raft Coordinator hosting CoordinatorService and GroupCoordinatorService.

steps:
1. allocate free TCP ports for Raft consensus and gRPC traffic.
2. initialize the Raft Coordinator server and GroupCoordinator server in a temporary directory.
3. serve both gRPC services and wait until the Raft node elects itself leader.

Returns:
- the Coordinator gRPC listen address and a CoordinatorServiceClient.
*/
func startTestCoordinator(t *testing.T) (string, ravenmqv1.CoordinatorServiceClient) {
	t.Helper()

	raftAddr := freeTCPAddr(t)
	grpcAddr := freeTCPAddr(t)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("listen coordinator grpc: %v", err)
	}

	coordSrv, err := coordinator.NewServer(coordinator.Config{
		NodeID:    "coord-test-1",
		RaftAddr:  raftAddr,
		GrpcAddr:  grpcAddr,
		DataDir:   t.TempDir(),
		Bootstrap: true,
	})
	if err != nil {
		t.Fatalf("new coordinator server: %v", err)
	}

	groupSrv := groupcoord.NewServer()

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterCoordinatorServiceServer(grpcServer, coordSrv)
	ravenmqv1.RegisterGroupCoordinatorServiceServer(grpcServer, groupSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	client := ravenmqv1.NewCoordinatorServiceClient(conn)

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = coordSrv.Close()
	})

	// Wait for Raft leader election before returning.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := client.RegisterBroker(ctx, &ravenmqv1.RegisterBrokerRequest{
			BrokerId: 1,
			GrpcAddr: "127.0.0.1:0",
		})
		cancel()
		if err == nil && resp.GetSuccess() {
			return grpcAddr, client
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("coordinator failed to elect leader within timeout")
	return "", nil
}

/*
startTestBroker boots a Broker gRPC server backed by a temporary commit log directory and registers it with the Coordinator.

steps:
1. bind a TCP listener on 127.0.0.1:0 and initialize the Broker server.
2. serve BrokerService in a background goroutine and register cleanup hooks.
3. register the broker's ID and gRPC address with the Coordinator.

Returns:
- the Broker's gRPC listen address.
*/
func startTestBroker(t *testing.T, coordClient ravenmqv1.CoordinatorServiceClient, brokerID int32) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen broker %d: %v", brokerID, err)
	}
	addr := lis.Addr().String()

	srv, err := broker.NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("new broker %d: %v", brokerID, err)
	}

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterBrokerServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = srv.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := coordClient.RegisterBroker(ctx, &ravenmqv1.RegisterBrokerRequest{
		BrokerId: brokerID,
		GrpcAddr: addr,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("register broker %d failed: resp=%+v, err=%v", brokerID, resp, err)
	}

	return addr
}

/*
TestEndToEnd_ProduceAndConsumerGroupRebalance verifies the full distributed workflow across Coordinator, Brokers, Producer, and Consumer group members.

steps:
1. start a Coordinator cluster and two Broker nodes, then create a 2-partition topic.
2. publish 6 keyed messages via Producer SDK so records land across both partitions and brokers.
3. start two Consumer SDK instances in the same group and wait until all 6 messages are consumed.
4. verify that every message was delivered once and that both consumers shared partition ownership.
*/
func TestEndToEnd_ProduceAndConsumerGroupRebalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	coordAddr, coordClient := startTestCoordinator(t)
	startTestBroker(t, coordClient, 1)
	startTestBroker(t, coordClient, 2)

	createResp, err := coordClient.CreateTopic(ctx, &ravenmqv1.CreateTopicRequest{
		Topic:      "orders",
		Partitions: 2,
	})
	if err != nil || createResp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		t.Fatalf("CreateTopic failed: resp=%+v, err=%v", createResp, err)
	}

	producer, err := ravenmq.NewProducer(coordAddr, 100)
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}
	defer func() { _ = producer.Close() }()

	const totalMessages = 6
	seenPartitions := make(map[int32]int)
	for i := 0; i < totalMessages; i++ {
		key := fmt.Appendf(nil, "order-key-%d", i)
		val := fmt.Appendf(nil, "payload-%d", i)
		_, partID, err := producer.Send(ctx, "orders", key, val)
		if err != nil {
			t.Fatalf("producer.Send(%d) failed: %v", i, err)
		}
		seenPartitions[partID]++
	}
	if len(seenPartitions) < 2 {
		t.Fatalf("expected producer to route across 2 partitions, got %+v", seenPartitions)
	}

	c1, err := ravenmq.NewConsumer(coordAddr, "billing-workers", "orders")
	if err != nil {
		t.Fatalf("NewConsumer 1 failed: %v", err)
	}
	defer func() { _ = c1.Close() }()

	c2, err := ravenmq.NewConsumer(coordAddr, "billing-workers", "orders")
	if err != nil {
		t.Fatalf("NewConsumer 2 failed: %v", err)
	}
	defer func() { _ = c2.Close() }()

	var (
		mu              sync.Mutex
		received        = make(map[string]int)
		consumer1Parts  = make(map[int32]bool)
		consumer2Parts  = make(map[int32]bool)
		allReceivedDone = make(chan struct{})
		once            sync.Once
	)

	recordMsg := func(consumerIdx int, msg ravenmq.Message) {
		mu.Lock()
		defer mu.Unlock()
		received[string(msg.Value)]++
		if consumerIdx == 1 {
			consumer1Parts[msg.Partition] = true
		} else {
			consumer2Parts[msg.Partition] = true
		}
		if len(received) == totalMessages {
			once.Do(func() { close(allReceivedDone) })
		}
	}

	runCtx, cancelConsumers := context.WithCancel(ctx)
	defer cancelConsumers()

	errCh := make(chan error, 2)
	go func() {
		errCh <- c1.Run(runCtx, func(_ context.Context, msg ravenmq.Message) error {
			recordMsg(1, msg)
			return nil
		})
	}()
	go func() {
		errCh <- c2.Run(runCtx, func(_ context.Context, msg ravenmq.Message) error {
			recordMsg(2, msg)
			return nil
		})
	}()

	select {
	case <-allReceivedDone:
		cancelConsumers()
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer exited with error: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for consumers; received=%d/%d (%+v)", len(received), totalMessages, received)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != totalMessages {
		t.Fatalf("expected %d unique messages, got %d: %+v", totalMessages, len(received), received)
	}
	for payload, count := range received {
		if count != 1 {
			t.Fatalf("expected message %q once, got %d", payload, count)
		}
	}
	if len(consumer1Parts) != 1 || len(consumer2Parts) != 1 {
		t.Fatalf("expected each consumer to own 1 partition; c1=%+v c2=%+v", consumer1Parts, consumer2Parts)
	}
}

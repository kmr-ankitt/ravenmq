package coordinator

import (
	"context"
	"net"
	"testing"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func getFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free addr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func setupTestCoordinator(t *testing.T, nodeID string, bootstrap bool) (*Server, ravenmqv1.CoordinatorServiceClient) {
	t.Helper()

	raftAddr := getFreeAddr(t)
	lis := bufconn.Listen(1024 * 1024)

	srv, err := NewServer(Config{
		NodeID:    nodeID,
		RaftAddr:  raftAddr,
		GrpcAddr:  "bufnet",
		DataDir:   t.TempDir(),
		Bootstrap: bootstrap,
	})
	if err != nil {
		t.Fatalf("NewServer(%s) failed: %v", nodeID, err)
	}

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterCoordinatorServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
		_ = srv.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return srv, ravenmqv1.NewCoordinatorServiceClient(conn)
}

func waitForLeader(t *testing.T, srv *Server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.raft.Leader() != "" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Raft leader election")
}

func TestCoordinator_RegisterBrokerCreateTopicAndMetadata(t *testing.T) {
	ctx := context.Background()
	srv, client := setupTestCoordinator(t, "node-1", true)
	waitForLeader(t, srv, 5*time.Second)

	// 1. Creating a topic before any broker is registered must fail
	if _, err := client.CreateTopic(ctx, &ravenmqv1.CreateTopicRequest{
		Topic:      "orders",
		Partitions: 2,
	}); status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error when creating topic with zero brokers, got %v", err)
	}

	// 2. Register two brokers
	for _, b := range []struct {
		id   int32
		addr string
	}{
		{1, "127.0.0.1:9001"},
		{2, "127.0.0.1:9002"},
	} {
		regResp, err := client.RegisterBroker(ctx, &ravenmqv1.RegisterBrokerRequest{
			BrokerId: b.id,
			GrpcAddr: b.addr,
		})
		if err != nil || !regResp.GetSuccess() {
			t.Fatalf("RegisterBroker(%d) failed: resp=%+v, err=%v", b.id, regResp, err)
		}
	}

	// 3. Create topic with 3 partitions -> deterministic round-robin assignment [1, 2, 1]
	createResp, err := client.CreateTopic(ctx, &ravenmqv1.CreateTopicRequest{
		Topic:      "orders",
		Partitions: 3,
	})
	if err != nil || createResp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		t.Fatalf("CreateTopic failed: resp=%+v, err=%v", createResp, err)
	}

	// 4. Verify GetClusterMetadata returns both brokers and deterministic partition leaders
	meta, err := client.GetClusterMetadata(ctx, &ravenmqv1.GetClusterMetadataRequest{})
	if err != nil {
		t.Fatalf("GetClusterMetadata failed: %v", err)
	}
	if len(meta.GetBrokers()) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(meta.GetBrokers()))
	}
	if len(meta.GetTopics()) != 1 || meta.GetTopics()[0].GetTopic() != "orders" {
		t.Fatalf("unexpected topics in metadata: %+v", meta.GetTopics())
	}

	expectedLeaders := map[int32]int32{0: 1, 1: 2, 2: 1}
	for _, p := range meta.GetTopics()[0].GetPartitions() {
		if want := expectedLeaders[p.GetPartitionId()]; p.GetLeaderBrokerId() != want {
			t.Fatalf("partition %d: expected leader %d, got %d", p.GetPartitionId(), want, p.GetLeaderBrokerId())
		}
	}
}

func TestCoordinator_JoinClusterAndReplication(t *testing.T) {
	ctx := context.Background()
	leaderSrv, leaderClient := setupTestCoordinator(t, "node-1", true)
	waitForLeader(t, leaderSrv, 5*time.Second)

	followerSrv, followerClient := setupTestCoordinator(t, "node-2", false)

	// Follower forwards/returns NOT_LEADER when asked to create a topic directly
	notLeaderResp, err := followerClient.CreateTopic(ctx, &ravenmqv1.CreateTopicRequest{
		Topic:      "payments",
		Partitions: 1,
	})
	if err != nil || notLeaderResp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NOT_LEADER {
		t.Fatalf("expected ERROR_CODE_NOT_LEADER from follower, got resp=%+v, err=%v", notLeaderResp, err)
	}

	// Join node-2 to node-1's Raft cluster
	joinResp, err := leaderClient.JoinCluster(ctx, &ravenmqv1.JoinClusterRequest{
		NodeId:   "node-2",
		RaftAddr: followerSrv.config.RaftAddr,
		GrpcAddr: "bufnet-2",
	})
	if err != nil || !joinResp.GetSuccess() {
		t.Fatalf("JoinCluster failed: resp=%+v, err=%v", joinResp, err)
	}

	// Apply state on leader and verify replication to follower's FSM
	if _, err := leaderClient.RegisterBroker(ctx, &ravenmqv1.RegisterBrokerRequest{
		BrokerId: 1,
		GrpcAddr: "127.0.0.1:9001",
	}); err != nil {
		t.Fatalf("RegisterBroker failed: %v", err)
	}
	if _, err := leaderClient.CreateTopic(ctx, &ravenmqv1.CreateTopicRequest{
		Topic:      "payments",
		Partitions: 2,
	}); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := followerClient.GetClusterMetadata(ctx, &ravenmqv1.GetClusterMetadataRequest{})
		if err == nil && len(meta.GetBrokers()) == 1 && len(meta.GetTopics()) == 1 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Raft state to replicate to follower")
}
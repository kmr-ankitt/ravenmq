package broker

import (
	"context"
	"net"
	"testing"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestBroker(t *testing.T) ravenmqv1.BrokerServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterBrokerServiceServer(grpcServer, srv)

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

	return ravenmqv1.NewBrokerServiceClient(conn)
}

func TestBroker_ProduceIdempotencyAndConsume(t *testing.T) {
	ctx := context.Background()
	client := setupTestBroker(t)

	// 1. Produce first message
	p1, err := client.Produce(ctx, &ravenmqv1.ProduceRequest{
		Topic:          "orders",
		Partition:      0,
		Value:          []byte("order-1"),
		ProducerId:     10,
		SequenceNumber: 1,
		Ack:            ravenmqv1.AckLevel_ACK_LEVEL_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce 1 failed: %v", err)
	}
	if p1.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE || p1.GetOffset() != 0 {
		t.Fatalf("unexpected Produce 1 response: %+v", p1)
	}

	// 2. Retry same (producer_id, sequence_number) -> must return offset -1
	dup, err := client.Produce(ctx, &ravenmqv1.ProduceRequest{
		Topic:          "orders",
		Partition:      0,
		Value:          []byte("order-1"),
		ProducerId:     10,
		SequenceNumber: 1,
	})
	if err != nil {
		t.Fatalf("duplicate Produce failed: %v", err)
	}
	if dup.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE || dup.GetOffset() != -1 {
		t.Fatalf("expected offset -1 for duplicate, got %+v", dup)
	}

	// 3. Produce second message
	p2, err := client.Produce(ctx, &ravenmqv1.ProduceRequest{
		Topic:          "orders",
		Partition:      0,
		Value:          []byte("order-2"),
		ProducerId:     10,
		SequenceNumber: 2,
	})
	if err != nil {
		t.Fatalf("Produce 2 failed: %v", err)
	}
	if p2.GetOffset() <= 0 {
		t.Fatalf("expected positive offset for second message, got %d", p2.GetOffset())
	}

	// 4. Consume sequentially using returned nextOffset
	c1, err := client.Consume(ctx, &ravenmqv1.ConsumeRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    0,
	})
	if err != nil {
		t.Fatalf("Consume 1 failed: %v", err)
	}
	if c1.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE || string(c1.GetRecord().GetValue()) != "order-1" {
		t.Fatalf("unexpected Consume 1 response: %+v", c1)
	}

	c2, err := client.Consume(ctx, &ravenmqv1.ConsumeRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    c1.GetRecord().GetOffset(),
	})
	if err != nil {
		t.Fatalf("Consume 2 failed: %v", err)
	}
	if c2.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE || string(c2.GetRecord().GetValue()) != "order-2" {
		t.Fatalf("unexpected Consume 2 response: %+v", c2)
	}

	// 5. Consume at EOF -> ERROR_CODE_OFFSET_OUT_OF_BOUNDS
	eof, err := client.Consume(ctx, &ravenmqv1.ConsumeRequest{
		Topic:     "orders",
		Partition: 0,
		Offset:    c2.GetRecord().GetOffset(),
	})
	if err != nil {
		t.Fatalf("Consume EOF failed: %v", err)
	}
	if eof.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_BOUNDS {
		t.Fatalf("expected ERROR_CODE_OFFSET_OUT_OF_BOUNDS, got %v", eof.GetErrorCode())
	}
}

func TestBroker_InvalidArguments(t *testing.T) {
	ctx := context.Background()
	client := setupTestBroker(t)

	if _, err := client.Produce(ctx, &ravenmqv1.ProduceRequest{Topic: "", Partition: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for empty topic on Produce, got %v", err)
	}
	if _, err := client.Produce(ctx, &ravenmqv1.ProduceRequest{Topic: "orders", Partition: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for negative partition on Produce, got %v", err)
	}
	if _, err := client.Consume(ctx, &ravenmqv1.ConsumeRequest{Topic: "orders", Partition: 0, Offset: -5}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for negative offset on Consume, got %v", err)
	}
}
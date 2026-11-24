package groupcoord

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

/*
setupTestGroupCoordinator starts an in-memory gRPC server for GroupCoordinatorService.

steps:
1. initialize a GroupCoordinator server with a short session timeout for fast expiration tests.
2. bind a bufconn listener and serve GroupCoordinatorService in a background goroutine.
3. open an insecure gRPC client connection and register cleanup hooks.

Returns:
- the initialized Server pointer and a GroupCoordinatorServiceClient.
*/
func setupTestGroupCoordinator(t *testing.T, sessionTimeout time.Duration) (*Server, ravenmqv1.GroupCoordinatorServiceClient) {
	t.Helper()

	srv := NewServer()
	srv.sessionTimeout = sessionTimeout

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterGroupCoordinatorServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
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

	return srv, ravenmqv1.NewGroupCoordinatorServiceClient(conn)
}

/*
TestGroupCoordinator_JoinSyncAndRebalance verifies leader election, partition assignment, and session timeout eviction.

steps:
1. join two consumers into the same group and confirm leader election and generation bump.
2. submit partition assignments from the leader via SyncGroup and verify both members receive their partitions.
3. send valid heartbeats for both members, then let the follower's session expire.
4. verify the leader's next heartbeat detects the eviction and returns ERROR_CODE_REBALANCE_IN_PROGRESS.
*/
func TestGroupCoordinator_JoinSyncAndRebalance(t *testing.T) {
	ctx := context.Background()
	_, client := setupTestGroupCoordinator(t, 150*time.Millisecond)

	// 1. Consumer 1 joins -> becomes group leader (generation 1)
	join1, err := client.JoinGroup(ctx, &ravenmqv1.JoinGroupRequest{
		GroupId: "billing-group",
		Topic:   "orders",
	})
	if err != nil || join1.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		t.Fatalf("consumer 1 JoinGroup failed: resp=%+v, err=%v", join1, err)
	}
	if join1.GetLeaderId() != join1.GetMemberId() || join1.GetGenerationId() != 1 {
		t.Fatalf("expected consumer 1 to be leader at generation 1, got %+v", join1)
	}

	// 2. Consumer 2 joins -> bumps generation to 2; follower receives empty members slice
	join2, err := client.JoinGroup(ctx, &ravenmqv1.JoinGroupRequest{
		GroupId: "billing-group",
		Topic:   "orders",
	})
	if err != nil || join2.GetGenerationId() != 2 {
		t.Fatalf("consumer 2 JoinGroup failed: resp=%+v, err=%v", join2, err)
	}
	if len(join2.GetMembers()) != 0 {
		t.Fatalf("expected follower JoinGroup to omit member list, got %+v", join2.GetMembers())
	}

	// 3. Leader re-joins with its assigned member_id to fetch the updated member list
	rejoin1, err := client.JoinGroup(ctx, &ravenmqv1.JoinGroupRequest{
		GroupId:  "billing-group",
		MemberId: join1.GetMemberId(),
		Topic:    "orders",
	})
	if err != nil || len(rejoin1.GetMembers()) != 2 {
		t.Fatalf("leader re-join expected 2 members, got resp=%+v, err=%v", rejoin1, err)
	}

	// 4. Leader syncs partition assignments for both members
	sync1, err := client.SyncGroup(ctx, &ravenmqv1.SyncGroupRequest{
		GroupId:      "billing-group",
		MemberId:     join1.GetMemberId(),
		GenerationId: rejoin1.GetGenerationId(),
		Assignments: []*ravenmqv1.MemberAssignment{
			{MemberId: join1.GetMemberId(), Partitions: []int32{0, 1}},
			{MemberId: join2.GetMemberId(), Partitions: []int32{2, 3}},
		},
	})
	if err != nil || !slices.Equal(sync1.GetAssignedPartitions(), []int32{0, 1}) {
		t.Fatalf("leader SyncGroup mismatch: resp=%+v, err=%v", sync1, err)
	}

	sync2, err := client.SyncGroup(ctx, &ravenmqv1.SyncGroupRequest{
		GroupId:      "billing-group",
		MemberId:     join2.GetMemberId(),
		GenerationId: rejoin1.GetGenerationId(),
	})
	if err != nil || !slices.Equal(sync2.GetAssignedPartitions(), []int32{2, 3}) {
		t.Fatalf("follower SyncGroup mismatch: resp=%+v, err=%v", sync2, err)
	}

	// 5. Heartbeat succeeds while both consumers are active
	hb1, err := client.Heartbeat(ctx, &ravenmqv1.HeartbeatRequest{
		GroupId:      "billing-group",
		MemberId:     join1.GetMemberId(),
		GenerationId: rejoin1.GetGenerationId(),
	})
	if err != nil || hb1.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		t.Fatalf("expected healthy heartbeat, got resp=%+v, err=%v", hb1, err)
	}

	// 6. Wait past sessionTimeout without heartbeats from consumer 2 -> triggers rebalance
	time.Sleep(200 * time.Millisecond)

	hbRebalance, err := client.Heartbeat(ctx, &ravenmqv1.HeartbeatRequest{
		GroupId:      "billing-group",
		MemberId:     join1.GetMemberId(),
		GenerationId: rejoin1.GetGenerationId(),
	})
	if err != nil || hbRebalance.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS {
		t.Fatalf("expected ERROR_CODE_REBALANCE_IN_PROGRESS after member timeout, got resp=%+v, err=%v", hbRebalance, err)
	}
}

/*
TestGroupCoordinator_CommitAndFetchOffset verifies consumer offset checkpointing and retrieval.

steps:
1. fetch an offset for a partition with no prior commits and confirm it defaults to 0.
2. commit a non-zero byte offset for the group, topic, and partition.
3. fetch the offset again and confirm the exact committed value is returned.
*/
func TestGroupCoordinator_CommitAndFetchOffset(t *testing.T) {
	ctx := context.Background()
	_, client := setupTestGroupCoordinator(t, defaultSessionTimeout)

	initial, err := client.FetchOffset(ctx, &ravenmqv1.FetchOffsetRequest{
		GroupId:   "analytics-group",
		Topic:     "events",
		Partition: 0,
	})
	if err != nil || initial.GetOffset() != 0 {
		t.Fatalf("expected initial offset 0, got resp=%+v, err=%v", initial, err)
	}

	commitResp, err := client.CommitOffset(ctx, &ravenmqv1.CommitOffsetRequest{
		GroupId:   "analytics-group",
		Topic:     "events",
		Partition: 0,
		Offset:    1024,
	})
	if err != nil || commitResp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		t.Fatalf("CommitOffset failed: resp=%+v, err=%v", commitResp, err)
	}

	fetched, err := client.FetchOffset(ctx, &ravenmqv1.FetchOffsetRequest{
		GroupId:   "analytics-group",
		Topic:     "events",
		Partition: 0,
	})
	if err != nil || fetched.GetOffset() != 1024 {
		t.Fatalf("expected fetched offset 1024, got resp=%+v, err=%v", fetched, err)
	}
}
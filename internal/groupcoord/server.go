package groupcoord

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultSessionTimeout = 10 * time.Second

type memberState struct {
	id         string
	lastSeen   time.Time
	partitions []int32
}

type groupState struct {
	id           string
	topic        string
	generationID int32
	leaderID     string
	members      map[string]*memberState
}

/*
Server implements GroupCoordinatorService for consumer group membership, partition rebalancing, and offset tracking.
*/
type Server struct {
	ravenmqv1.UnimplementedGroupCoordinatorServiceServer
	mu             sync.Mutex
	groups         map[string]*groupState
	offsets        map[string]uint64 // key: "groupID:topic:partition" -> byte offset
	sessionTimeout time.Duration
	memberSeq      atomic.Uint64
}

/*
NewServer initializes a new GroupCoordinator server with the default session timeout.

steps:
1. allocate maps for active consumer groups and committed partition offsets.
2. configure the default heartbeat session timeout.

Returns:
- an initialized pointer to Server.
*/
func NewServer() *Server {
	return &Server{
		groups:         make(map[string]*groupState),
		offsets:        make(map[string]uint64),
		sessionTimeout: defaultSessionTimeout,
	}
}

/*
offsetKey builds a unique lookup key for storing and fetching committed offsets.

steps:
1. format group ID, topic name, and partition ID into a colon-delimited string.

Returns:
- the formatted offset map key.
*/
func offsetKey(groupID, topic string, partition int32) string {
	return fmt.Sprintf("%s:%s:%d", groupID, topic, partition)
}

/*
evictExpiredLocked removes timed-out members from a consumer group and triggers a rebalance if any expired.

steps:
1. iterate over all members in the group and delete any whose lastSeen exceeds sessionTimeout.
2. if any member was evicted, increment generationID and re-elect a leader if the previous leader left.

Returns:
- true if one or more expired members were removed, false otherwise.
*/
func (s *Server) evictExpiredLocked(g *groupState, now time.Time) bool {
	evicted := false
	for id, m := range g.members {
		if now.Sub(m.lastSeen) > s.sessionTimeout {
			delete(g.members, id)
			evicted = true
		}
	}

	if evicted {
		g.generationID++
		if _, leaderAlive := g.members[g.leaderID]; !leaderAlive {
			g.leaderID = ""
			for id := range g.members {
				if g.leaderID == "" || id < g.leaderID {
					g.leaderID = id
				}
			}
		}
	}
	return evicted
}

/*
JoinGroup registers a consumer in a group, triggers a new generation, and returns the member list to the group leader.

steps:
1. validate group_id and topic, and generate a unique member_id if the client did not supply one.
2. evict any expired members and create the group state if it does not yet exist.
3. add or refresh the member, increment generation_id, and elect a deterministic group leader.
4. populate the sorted member list only for the elected group leader.

Returns:
- JoinGroupResponse containing the assigned member_id, generation_id, leader_id, and member list.
*/
func (s *Server) JoinGroup(ctx context.Context, req *ravenmqv1.JoinGroupRequest) (*ravenmqv1.JoinGroupResponse, error) {
	if req.GetGroupId() == "" || req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and topic are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	g, exists := s.groups[req.GetGroupId()]
	if !exists {
		g = &groupState{
			id:      req.GetGroupId(),
			topic:   req.GetTopic(),
			members: make(map[string]*memberState),
		}
		s.groups[req.GetGroupId()] = g
	} else {
		s.evictExpiredLocked(g, now)
	}

	memberID := req.GetMemberId()
	if memberID == "" {
		seq := s.memberSeq.Add(1)
		memberID = fmt.Sprintf("%s-member-%d", req.GetGroupId(), seq)
	}

	if m, ok := g.members[memberID]; ok {
		m.lastSeen = now
	} else {
		g.members[memberID] = &memberState{
			id:       memberID,
			lastSeen: now,
		}
		g.generationID++
	}

	if g.leaderID == "" || g.members[g.leaderID] == nil {
		g.leaderID = memberID
	}

	var members []string
	if memberID == g.leaderID {
		members = make([]string, 0, len(g.members))
		for id := range g.members {
			members = append(members, id)
		}
		slices.Sort(members)
	}

	return &ravenmqv1.JoinGroupResponse{
		ErrorCode:    ravenmqv1.ErrorCode_ERROR_CODE_NONE,
		MemberId:     memberID,
		GenerationId: g.generationID,
		LeaderId:     g.leaderID,
		Members:      members,
	}, nil
}

/*
SyncGroup distributes partition assignments computed by the group leader to all members in the current generation.

steps:
1. validate group_id and member_id and verify the group and member exist.
2. check that the request generation_id matches the group's current generation_id.
3. if the caller is the group leader, save the submitted partition assignments onto each member.
4. return the caller's assigned partitions.

Returns:
- SyncGroupResponse containing the error code and the member's assigned partitions.
*/
func (s *Server) SyncGroup(ctx context.Context, req *ravenmqv1.SyncGroupRequest) (*ravenmqv1.SyncGroupResponse, error) {
	if req.GetGroupId() == "" || req.GetMemberId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and member_id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.groups[req.GetGroupId()]
	if !exists {
		return nil, status.Error(codes.NotFound, "consumer group not found")
	}

	member, ok := g.members[req.GetMemberId()]
	if !ok {
		return &ravenmqv1.SyncGroupResponse{
			ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS,
		}, nil
	}

	if req.GetGenerationId() != g.generationID {
		return &ravenmqv1.SyncGroupResponse{
			ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS,
		}, nil
	}

	member.lastSeen = time.Now()

	if req.GetMemberId() == g.leaderID && len(req.GetAssignments()) > 0 {
		for _, a := range req.GetAssignments() {
			if target, found := g.members[a.GetMemberId()]; found {
				target.partitions = slices.Clone(a.GetPartitions())
			}
		}
	}

	return &ravenmqv1.SyncGroupResponse{
		ErrorCode:          ravenmqv1.ErrorCode_ERROR_CODE_NONE,
		AssignedPartitions: slices.Clone(member.partitions),
	}, nil
}

/*
Heartbeat refreshes a consumer's session liveness and signals if a group rebalance is required.

steps:
1. validate group_id and member_id and locate the group.
2. evict any expired members in the group.
3. verify the member is still active and its generation_id matches the group's generation_id.
4. update the member's lastSeen timestamp and return success.

Returns:
- HeartbeatResponse with ERROR_CODE_NONE or ERROR_CODE_REBALANCE_IN_PROGRESS.
*/
func (s *Server) Heartbeat(ctx context.Context, req *ravenmqv1.HeartbeatRequest) (*ravenmqv1.HeartbeatResponse, error) {
	if req.GetGroupId() == "" || req.GetMemberId() == "" {
		return nil, status.Error(codes.InvalidArgument, "group_id and member_id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	g, exists := s.groups[req.GetGroupId()]
	if !exists {
		return nil, status.Error(codes.NotFound, "consumer group not found")
	}

	now := time.Now()
	s.evictExpiredLocked(g, now)

	member, ok := g.members[req.GetMemberId()]
	if !ok || req.GetGenerationId() != g.generationID {
		return &ravenmqv1.HeartbeatResponse{
			ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS,
		}, nil
	}

	member.lastSeen = now
	return &ravenmqv1.HeartbeatResponse{
		ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_NONE,
	}, nil
}

/*
CommitOffset checkpoints the next byte offset to consume for a consumer group, topic, and partition.

steps:
1. validate group_id, topic, and partition.
2. store the offset in the offsets map under the composite group/topic/partition key.

Returns:
- CommitOffsetResponse confirming the offset was saved.
*/
func (s *Server) CommitOffset(ctx context.Context, req *ravenmqv1.CommitOffsetRequest) (*ravenmqv1.CommitOffsetResponse, error) {
	if req.GetGroupId() == "" || req.GetTopic() == "" || req.GetPartition() < 0 {
		return nil, status.Error(codes.InvalidArgument, "group_id, topic, and non-negative partition are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.offsets[offsetKey(req.GetGroupId(), req.GetTopic(), req.GetPartition())] = req.GetOffset()
	return &ravenmqv1.CommitOffsetResponse{
		ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_NONE,
	}, nil
}

/*
FetchOffset retrieves the last committed byte offset for a consumer group, topic, and partition.

steps:
1. validate group_id, topic, and partition.
2. look up the stored offset (defaulting to 0 if none has been committed yet).

Returns:
- FetchOffsetResponse containing the byte offset.
*/
func (s *Server) FetchOffset(ctx context.Context, req *ravenmqv1.FetchOffsetRequest) (*ravenmqv1.FetchOffsetResponse, error) {
	if req.GetGroupId() == "" || req.GetTopic() == "" || req.GetPartition() < 0 {
		return nil, status.Error(codes.InvalidArgument, "group_id, topic, and non-negative partition are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.offsets[offsetKey(req.GetGroupId(), req.GetTopic(), req.GetPartition())]
	return &ravenmqv1.FetchOffsetResponse{
		ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_NONE,
		Offset:    offset,
	}, nil
}

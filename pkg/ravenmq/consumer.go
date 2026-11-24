package ravenmq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultHeartbeatInterval = 3 * time.Second
	defaultPollInterval      = 100 * time.Millisecond
)

/*
Message represents a single record delivered to a Consumer handler.
*/
type Message struct {
	Topic     string
	Partition int32
	Offset    uint64
	Key       []byte
	Value     []byte
}

/*
MessageHandler processes a consumed Message; returning nil commits the offset.
*/
type MessageHandler func(ctx context.Context, msg Message) error

/*
Consumer subscribes to a topic as part of a consumer group, handling rebalancing, heartbeats, and offset commits.
*/
type Consumer struct {
	groupID     string
	topic       string
	coordConn   *grpc.ClientConn
	coordClient ravenmqv1.CoordinatorServiceClient
	groupClient ravenmqv1.GroupCoordinatorServiceClient

	mu             sync.RWMutex
	memberID       string
	generationID   int32
	partitions     []int32
	offsets        map[int32]uint64
	partitionAddrs map[int32]string
	brokerConns    map[string]*grpc.ClientConn
	brokerStubs    map[string]ravenmqv1.BrokerServiceClient

	needsRebalance atomic.Bool
}

/*
NewConsumer initializes a Consumer connected to the Coordinator and GroupCoordinator services.

steps:
1. open an insecure gRPC client connection to the Coordinator address.
2. instantiate CoordinatorService and GroupCoordinatorService clients and internal tracking maps.

Returns:
- an initialized Consumer pointer or an error if dialing fails.
*/
func NewConsumer(coordAddr, groupID, topic string) (*Consumer, error) {
	if groupID == "" || topic == "" {
		return nil, errors.New("groupID and topic are required")
	}

	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial coordinator %s: %w", coordAddr, err)
	}

	c := &Consumer{
		groupID:        groupID,
		topic:          topic,
		coordConn:      conn,
		coordClient:    ravenmqv1.NewCoordinatorServiceClient(conn),
		groupClient:    ravenmqv1.NewGroupCoordinatorServiceClient(conn),
		offsets:        make(map[int32]uint64),
		partitionAddrs: make(map[int32]string),
		brokerConns:    make(map[string]*grpc.ClientConn),
		brokerStubs:    make(map[string]ravenmqv1.BrokerServiceClient),
	}
	c.needsRebalance.Store(true)
	return c, nil
}

/*
refreshPartitionLeaders updates the local map of partition IDs to leader broker gRPC addresses.

steps:
1. call GetClusterMetadata on the Coordinator.
2. map broker IDs to addresses and record the leader address for every partition in the subscribed topic.

Returns:
- the sorted slice of all partition IDs for the topic, or an error if metadata lookup fails.
*/
func (c *Consumer) refreshPartitionLeaders(ctx context.Context) ([]int32, error) {
	meta, err := c.coordClient.GetClusterMetadata(ctx, &ravenmqv1.GetClusterMetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("get cluster metadata: %w", err)
	}

	brokerAddrs := make(map[int32]string, len(meta.GetBrokers()))
	for _, b := range meta.GetBrokers() {
		brokerAddrs[b.GetBrokerId()] = b.GetGrpcAddr()
	}

	var allPartitions []int32
	updatedAddrs := make(map[int32]string)
	for _, t := range meta.GetTopics() {
		if t.GetTopic() != c.topic {
			continue
		}
		for _, p := range t.GetPartitions() {
			allPartitions = append(allPartitions, p.GetPartitionId())
			if addr, ok := brokerAddrs[p.GetLeaderBrokerId()]; ok {
				updatedAddrs[p.GetPartitionId()] = addr
			}
		}
	}
	slices.Sort(allPartitions)

	c.mu.Lock()
	c.partitionAddrs = updatedAddrs
	c.mu.Unlock()

	return allPartitions, nil
}

/*
buildRoundRobinAssignments distributes topic partitions evenly across all active group members.

steps:
1. initialize an assignment entry for each member ID.
2. assign each partition index modulo the member count to balance load deterministically.

Returns:
- a slice of MemberAssignment protobuf messages ready for SyncGroup.
*/
func buildRoundRobinAssignments(members []string, partitions []int32) []*ravenmqv1.MemberAssignment {
	assignments := make([]*ravenmqv1.MemberAssignment, len(members))
	for i, m := range members {
		assignments[i] = &ravenmqv1.MemberAssignment{MemberId: m}
	}
	if len(members) == 0 {
		return assignments
	}
	for i, partID := range partitions {
		target := i % len(members)
		assignments[target].Partitions = append(assignments[target].Partitions, partID)
	}
	return assignments
}

/*
rebalance executes the JoinGroup, SyncGroup, and FetchOffset protocol to claim partitions for this consumer.

steps:
1. refresh topic partition leaders from the Coordinator.
2. call JoinGroup (re-joining once if elected leader on initial join so all members are captured).
3. if this consumer is the group leader, compute round-robin partition assignments; otherwise pass nil assignments.
4. call SyncGroup to obtain assigned partitions and fetch the last committed offset for each partition.

Returns:
- an error if any step of the rebalance protocol fails, or nil on success.
*/
func (c *Consumer) rebalance(ctx context.Context) error {
	allPartitions, err := c.refreshPartitionLeaders(ctx)
	if err != nil {
		return err
	}

	c.mu.RLock()
	currentMemberID := c.memberID
	c.mu.RUnlock()

	joinResp, err := c.groupClient.JoinGroup(ctx, &ravenmqv1.JoinGroupRequest{
		GroupId:  c.groupID,
		MemberId: currentMemberID,
		Topic:    c.topic,
	})
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}

	memberID := joinResp.GetMemberId()
	genID := joinResp.GetGenerationId()
	members := joinResp.GetMembers()

	// Brief settle window on initial join so concurrent consumers join before leader assigns partitions.
	if currentMemberID == "" && joinResp.GetLeaderId() == memberID {
		time.Sleep(150 * time.Millisecond)
		rejoinResp, err := c.groupClient.JoinGroup(ctx, &ravenmqv1.JoinGroupRequest{
			GroupId:  c.groupID,
			MemberId: memberID,
			Topic:    c.topic,
		})
		if err != nil {
			return fmt.Errorf("leader rejoin group: %w", err)
		}
		genID = rejoinResp.GetGenerationId()
		members = rejoinResp.GetMembers()
	}

	var assignments []*ravenmqv1.MemberAssignment
	if joinResp.GetLeaderId() == memberID {
		assignments = buildRoundRobinAssignments(members, allPartitions)
	}

	syncResp, err := c.groupClient.SyncGroup(ctx, &ravenmqv1.SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: genID,
		Assignments:  assignments,
	})
	if err != nil {
		return fmt.Errorf("sync group: %w", err)
	}
	if syncResp.GetErrorCode() == ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS {
		c.needsRebalance.Store(true)
		return nil
	}

	assigned := slices.Clone(syncResp.GetAssignedPartitions())
	slices.Sort(assigned)

	if len(assigned) == 0 && len(allPartitions) > 0 {
		c.mu.Lock()
		c.memberID = memberID
		c.generationID = genID
		c.mu.Unlock()
		c.needsRebalance.Store(true)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	newOffsets := make(map[int32]uint64, len(assigned))
	for _, partID := range assigned {
		fetchResp, err := c.groupClient.FetchOffset(ctx, &ravenmqv1.FetchOffsetRequest{
			GroupId:   c.groupID,
			Topic:     c.topic,
			Partition: partID,
		})
		if err != nil {
			return fmt.Errorf("fetch offset for partition %d: %w", partID, err)
		}
		newOffsets[partID] = fetchResp.GetOffset()
	}

	c.mu.Lock()
	c.memberID = memberID
	c.generationID = genID
	c.partitions = assigned
	c.offsets = newOffsets
	c.mu.Unlock()

	c.needsRebalance.Store(false)
	return nil
}

/*
getBrokerClient returns a pooled BrokerServiceClient for a broker gRPC address, dialing on first use.

steps:
1. check the cached brokerStubs map under a read lock.
2. if missing, acquire a write lock, dial the broker address, and cache the connection and stub.

Returns:
- a BrokerServiceClient or an error if dialing fails.
*/
func (c *Consumer) getBrokerClient(addr string) (ravenmqv1.BrokerServiceClient, error) {
	c.mu.RLock()
	stub, ok := c.brokerStubs[addr]
	c.mu.RUnlock()
	if ok {
		return stub, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stub, ok := c.brokerStubs[addr]; ok {
		return stub, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial broker %s: %w", addr, err)
	}
	stub = ravenmqv1.NewBrokerServiceClient(conn)
	c.brokerConns[addr] = conn
	c.brokerStubs[addr] = stub
	return stub, nil
}

/*
heartbeatLoop periodically sends Heartbeat RPCs to keep the consumer session alive and detect rebalances.

steps:
1. tick every defaultHeartbeatInterval until ctx is canceled.
2. read the current memberID and generationID; skip if not yet joined.
3. send Heartbeat and set needsRebalance if the coordinator reports ERROR_CODE_REBALANCE_IN_PROGRESS.
*/
func (c *Consumer) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.needsRebalance.Load() {
				continue
			}
			c.mu.RLock()
			memberID := c.memberID
			genID := c.generationID
			c.mu.RUnlock()

			if memberID == "" {
				continue
			}

			resp, err := c.groupClient.Heartbeat(ctx, &ravenmqv1.HeartbeatRequest{
				GroupId:      c.groupID,
				MemberId:     memberID,
				GenerationId: genID,
			})
			if err == nil && resp.GetErrorCode() == ravenmqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS {
				c.needsRebalance.Store(true)
			}
		}
	}
}

/*
pollOnce reads up to one record from each assigned partition, invokes handler, and commits the next offset.

steps:
1. snapshot the consumer's assigned partitions, current byte offsets, and broker addresses.
2. for each partition, call Consume on its leader broker starting at the tracked byte offset.
3. when a record is returned, invoke handler; if handler returns nil, commit and advance the local offset.

Returns:
- true if at least one record was consumed, false if all partitions were at EOF, or an error on failure.
*/
func (c *Consumer) pollOnce(ctx context.Context, handler MessageHandler) (bool, error) {
	c.mu.RLock()
	parts := slices.Clone(c.partitions)
	offsets := make(map[int32]uint64, len(parts))
	addrs := make(map[int32]string, len(parts))
	for _, p := range parts {
		offsets[p] = c.offsets[p]
		addrs[p] = c.partitionAddrs[p]
	}
	c.mu.RUnlock()

	consumedAny := false
	for _, partID := range parts {
		addr := addrs[partID]
		if addr == "" {
			continue
		}
		brokerClient, err := c.getBrokerClient(addr)
		if err != nil {
			return consumedAny, err
		}

		currentOffset := offsets[partID]
		resp, err := brokerClient.Consume(ctx, &ravenmqv1.ConsumeRequest{
			Topic:     c.topic,
			Partition: partID,
			Offset:    int64(currentOffset),
		})
		if err != nil {
			return consumedAny, fmt.Errorf("consume %s[%d] at %d: %w", c.topic, partID, currentOffset, err)
		}
		if resp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE || resp.GetRecord() == nil {
			continue
		}

		rec := resp.GetRecord()
		msg := Message{
			Topic:     c.topic,
			Partition: partID,
			Offset:    currentOffset,
			Value:     rec.GetValue(),
		}
		if err := handler(ctx, msg); err != nil {
			return consumedAny, err
		}

		nextOffset := uint64(rec.GetOffset())
		_, err = c.groupClient.CommitOffset(ctx, &ravenmqv1.CommitOffsetRequest{
			GroupId:   c.groupID,
			Topic:     c.topic,
			Partition: partID,
			Offset:    nextOffset,
		})
		if err != nil {
			return consumedAny, fmt.Errorf("commit offset %d on %s[%d]: %w", nextOffset, c.topic, partID, err)
		}

		c.mu.Lock()
		c.offsets[partID] = nextOffset
		c.mu.Unlock()

		consumedAny = true
	}

	return consumedAny, nil
}

/*
Run starts the background heartbeat loop and continuously rebalances and polls messages until ctx is canceled.

steps:
1. spawn heartbeatLoop in a background goroutine.
2. loop until ctx is canceled: trigger rebalance whenever needsRebalance is true.
3. poll assigned partitions via pollOnce and sleep briefly when no new messages are available.

Returns:
- ctx.Err() on graceful cancellation, or an unrecoverable handler/client error.
*/
func (c *Consumer) Run(ctx context.Context, handler MessageHandler) error {
	go c.heartbeatLoop(ctx)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if c.needsRebalance.Load() {
			if err := c.rebalance(ctx); err != nil {
				return err
			}
		}

		consumed, err := c.pollOnce(ctx, handler)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		if !consumed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(defaultPollInterval):
			}
		}
	}
}

/*
Close closes the Coordinator connection and all pooled Broker gRPC connections.

steps:
1. close all cached broker connections under a write lock.
2. close the Coordinator gRPC connection and join any errors.

Returns:
- a combined error if any connection fails to close, or nil on success.
*/
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for addr, conn := range c.brokerConns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close broker conn %s: %w", addr, err))
		}
		delete(c.brokerConns, addr)
		delete(c.brokerStubs, addr)
	}
	if c.coordConn != nil {
		if err := c.coordConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close coordinator conn: %w", err))
		}
	}
	return errors.Join(errs...)
}

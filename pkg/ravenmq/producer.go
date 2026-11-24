package ravenmq

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type partitionRoute struct {
	partitionID int32
	leaderAddr  string
}

/*
Producer routes messages to partition leader brokers with automatic metadata discovery and idempotent sequence tracking.
*/
type Producer struct {
	producerID  uint64
	coordConn   *grpc.ClientConn
	coordClient ravenmqv1.CoordinatorServiceClient

	mu          sync.RWMutex
	topicRoutes map[string][]partitionRoute
	brokerConns map[string]*grpc.ClientConn
	brokerStubs map[string]ravenmqv1.BrokerServiceClient
	seqNums     map[string]uint64
	rrCounter   atomic.Uint64
}

/*
NewProducer initializes a Producer connected to the Coordinator cluster.

steps:
1. open an insecure gRPC client connection to the Coordinator address.
2. initialize internal caches for topic partition routes, broker connections, and sequence numbers.

Returns:
- an initialized Producer pointer or an error if dialing fails.
*/
func NewProducer(coordAddr string, producerID uint64) (*Producer, error) {
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial coordinator %s: %w", coordAddr, err)
	}

	return &Producer{
		producerID:  producerID,
		coordConn:   conn,
		coordClient: ravenmqv1.NewCoordinatorServiceClient(conn),
		topicRoutes: make(map[string][]partitionRoute),
		brokerConns: make(map[string]*grpc.ClientConn),
		brokerStubs: make(map[string]ravenmqv1.BrokerServiceClient),
		seqNums:     make(map[string]uint64),
	}, nil
}

/*
RefreshMetadata fetches the latest broker registry and topic partition leader mappings from the Coordinator.

steps:
1. call GetClusterMetadata on the Coordinator.
2. build a lookup map of broker_id to gRPC address.
3. construct sorted partitionRoute slices for every topic and update the producer's routing cache.

Returns:
- an error if the RPC fails, or nil on success.
*/
func (p *Producer) RefreshMetadata(ctx context.Context) error {
	resp, err := p.coordClient.GetClusterMetadata(ctx, &ravenmqv1.GetClusterMetadataRequest{})
	if err != nil {
		return fmt.Errorf("get cluster metadata: %w", err)
	}

	brokerAddrs := make(map[int32]string, len(resp.GetBrokers()))
	for _, b := range resp.GetBrokers() {
		brokerAddrs[b.GetBrokerId()] = b.GetGrpcAddr()
	}

	updated := make(map[string][]partitionRoute, len(resp.GetTopics()))
	for _, t := range resp.GetTopics() {
		routes := make([]partitionRoute, 0, len(t.GetPartitions()))
		for _, part := range t.GetPartitions() {
			addr, ok := brokerAddrs[part.GetLeaderBrokerId()]
			if !ok {
				continue
			}
			routes = append(routes, partitionRoute{
				partitionID: part.GetPartitionId(),
				leaderAddr:  addr,
			})
		}
		slices.SortFunc(routes, func(a, b partitionRoute) int {
			return int(a.partitionID - b.partitionID)
		})
		updated[t.GetTopic()] = routes
	}

	p.mu.Lock()
	p.topicRoutes = updated
	p.mu.Unlock()
	return nil
}

/*
getBrokerClient returns a pooled BrokerServiceClient for the target broker address, dialing on first use.

steps:
1. check the cached brokerStubs map under a read lock.
2. if not cached, acquire a write lock, dial the broker address, and store both the connection and client stub.

Returns:
- a BrokerServiceClient or an error if dialing fails.
*/
func (p *Producer) getBrokerClient(addr string) (ravenmqv1.BrokerServiceClient, error) {
	p.mu.RLock()
	stub, ok := p.brokerStubs[addr]
	p.mu.RUnlock()
	if ok {
		return stub, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if stub, ok := p.brokerStubs[addr]; ok {
		return stub, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial broker %s: %w", addr, err)
	}
	stub = ravenmqv1.NewBrokerServiceClient(conn)
	p.brokerConns[addr] = conn
	p.brokerStubs[addr] = stub
	return stub, nil
}

/*
selectPartition chooses a target partition route using FNV-1a key hashing or atomic round-robin.

steps:
1. if key is non-empty, compute its 32-bit FNV-1a hash modulo the partition count.
2. if key is empty, increment the atomic round-robin counter modulo the partition count.

Returns:
- the selected partitionRoute.
*/
func (p *Producer) selectPartition(routes []partitionRoute, key []byte) partitionRoute {
	n := uint64(len(routes))
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		return routes[uint64(h.Sum32())%n]
	}
	idx := p.rrCounter.Add(1) - 1
	return routes[idx%n]
}

/*
Send publishes a message to a topic, automatically routing to the partition leader and tracking sequence numbers.

steps:
1. look up cached partition routes for the topic, refreshing metadata from the Coordinator on cache miss.
2. select the target partition route via key hash or round-robin.
3. increment the sequence number for the (topic, partition) pair and dial/reuse the leader broker client.
4. send the Produce RPC and return the assigned byte offset and partition ID.

Returns:
- the written byte offset, target partition ID, and an error if metadata lookup or Produce fails.
*/
func (p *Producer) Send(ctx context.Context, topic string, key, value []byte) (int64, int32, error) {
	p.mu.RLock()
	routes := p.topicRoutes[topic]
	p.mu.RUnlock()

	if len(routes) == 0 {
		if err := p.RefreshMetadata(ctx); err != nil {
			return 0, 0, err
		}
		p.mu.RLock()
		routes = p.topicRoutes[topic]
		p.mu.RUnlock()
		if len(routes) == 0 {
			return 0, 0, fmt.Errorf("topic %q has no available partitions", topic)
		}
	}

	route := p.selectPartition(routes, key)

	p.mu.Lock()
	seqKey := fmt.Sprintf("%s:%d", topic, route.partitionID)
	p.seqNums[seqKey]++
	seq := p.seqNums[seqKey]
	p.mu.Unlock()

	brokerClient, err := p.getBrokerClient(route.leaderAddr)
	if err != nil {
		return 0, 0, err
	}

	resp, err := brokerClient.Produce(ctx, &ravenmqv1.ProduceRequest{
		Topic:          topic,
		Partition:      route.partitionID,
		Value:          value,
		ProducerId:     p.producerID,
		SequenceNumber: seq,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("produce to %s[%d]: %w", topic, route.partitionID, err)
	}
	if resp.GetErrorCode() != ravenmqv1.ErrorCode_ERROR_CODE_NONE {
		return 0, 0, fmt.Errorf("broker returned error code: %s", resp.GetErrorCode().String())
	}

	return resp.GetOffset(), route.partitionID, nil
}

/*
Close closes the Coordinator connection and all pooled Broker gRPC connections.

steps:
1. close all cached broker client connections under a write lock.
2. close the Coordinator gRPC client connection and join any errors.

Returns:
- a combined error if any connection fails to close, or nil on success.
*/
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for addr, conn := range p.brokerConns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close broker conn %s: %w", addr, err))
		}
		delete(p.brokerConns, addr)
		delete(p.brokerStubs, addr)
	}
	if p.coordConn != nil {
		if err := p.coordConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close coordinator conn: %w", err))
		}
	}
	return errors.Join(errs...)
}

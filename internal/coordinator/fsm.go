package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/raft"
)

type CommandType string

const (
	CmdRegisterBroker CommandType = "register_broker"
	CmdCreateTopic    CommandType = "create_topic"
)

type Command struct {
	Type       CommandType `json:"type"`
	BrokerID   int32       `json:"broker_id,omitempty"`
	GrpcAddr   string      `json:"grpc_addr,omitempty"`
	Topic      string      `json:"topic,omitempty"`
	Partitions int32       `json:"partitions,omitempty"`
}

type TopicState struct {
	Name       string          `json:"name"`
	Partitions map[int32]int32 `json:"partitions"` // partition_id -> leader_broker_id
}

type FSM struct {
	mu      sync.RWMutex
	brokers map[int32]string       // broker_id -> grpc_addr
	topics  map[string]*TopicState // topic -> state
}

func NewFSM() *FSM {
	return &FSM{
		brokers: make(map[int32]string),
		topics:  make(map[string]*TopicState),
	}
}

func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal raft command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdRegisterBroker:
		f.brokers[cmd.BrokerID] = cmd.GrpcAddr
		return nil

	case CmdCreateTopic:
		if len(f.brokers) == 0 {
			return fmt.Errorf("no brokers registered in cluster")
		}
		if _, exists := f.topics[cmd.Topic]; exists {
			return nil
		}

		brokerIDs := make([]int32, 0, len(f.brokers))
		for id := range f.brokers {
			brokerIDs = append(brokerIDs, id)
		}
		slices.Sort(brokerIDs)

		partitions := make(map[int32]int32, cmd.Partitions)
		for p := int32(0); p < cmd.Partitions; p++ {
			partitions[p] = brokerIDs[int(p)%len(brokerIDs)]
		}

		f.topics[cmd.Topic] = &TopicState{
			Name:       cmd.Topic,
			Partitions: partitions,
		}
		return nil

	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	brokersCopy := make(map[int32]string, len(f.brokers))
	for id, addr := range f.brokers {
		brokersCopy[id] = addr
	}

	topicsCopy := make(map[string]*TopicState, len(f.topics))
	for name, topic := range f.topics {
		partsCopy := make(map[int32]int32, len(topic.Partitions))
		for p, leader := range topic.Partitions {
			partsCopy[p] = leader
		}
		topicsCopy[name] = &TopicState{
			Name:       topic.Name,
			Partitions: partsCopy,
		}
	}

	return &fsmSnapshot{
		Brokers: brokersCopy,
		Topics:  topicsCopy,
	}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snap fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if snap.Brokers == nil {
		snap.Brokers = make(map[int32]string)
	}
	if snap.Topics == nil {
		snap.Topics = make(map[string]*TopicState)
	}
	f.brokers = snap.Brokers
	f.topics = snap.Topics
	return nil
}

func (f *FSM) Metadata() (map[int32]string, map[string]*TopicState) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	brokers := make(map[int32]string, len(f.brokers))
	for id, addr := range f.brokers {
		brokers[id] = addr
	}

	topics := make(map[string]*TopicState, len(f.topics))
	for name, topic := range f.topics {
		parts := make(map[int32]int32, len(topic.Partitions))
		for p, leader := range topic.Partitions {
			parts[p] = leader
		}
		topics[name] = &TopicState{
			Name:       topic.Name,
			Partitions: parts,
		}
	}
	return brokers, topics
}

type fsmSnapshot struct {
	Brokers map[int32]string       `json:"brokers"`
	Topics  map[string]*TopicState `json:"topics"`
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(s)
		if err != nil {
			return err
		}
		if _, err := sink.Write(data); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		_ = sink.Cancel()
	}
	return err
}

func (s *fsmSnapshot) Release() {}

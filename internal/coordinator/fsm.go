package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/hashicorp/raft"
)

// CommandType identifies an operation applied to the coordinator FSM.
type CommandType string

const (
	CmdRegisterBroker CommandType = "register_broker"
	CmdCreateTopic    CommandType = "create_topic"
)

// Command represents a state change replicated through Raft.
type Command struct {
	Type       CommandType `json:"type"`
	BrokerID   int32       `json:"broker_id,omitempty"`
	GrpcAddr   string      `json:"grpc_addr,omitempty"`
	Topic      string      `json:"topic,omitempty"`
	Partitions int32       `json:"partitions,omitempty"`
}

// TopicState stores a topic's partition leaders.
type TopicState struct {
	Name       string          `json:"name"`
	Partitions map[int32]int32 `json:"partitions"` // partition_id -> leader_broker_id
}

// FSM maintains broker and topic metadata replicated through Raft.
type FSM struct {
	mu      sync.RWMutex
	brokers map[int32]string       // broker_id -> grpc_addr
	topics  map[string]*TopicState // topic -> state
}

/*
NewFSM creates an initialized coordinator state machine.

steps:
 1. allocate the broker and topic maps.
 2. return the new FSM.

Returns:
  - initialized FSM.
*/
func NewFSM() *FSM {
	return &FSM{
		brokers: make(map[int32]string),
		topics:  make(map[string]*TopicState),
	}
}

/*
Apply applies a Raft log entry to the coordinator state.

steps:
 1. decode the command from the log data.
 2. lock the FSM state.
 3. register the broker or create the topic according to the command.

Returns:
  - an error for invalid commands or failed topic creation; otherwise nil.
*/
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

/*
Snapshot creates a consistent copy of the coordinator state.

steps:
 1. read-lock the FSM state.
 2. copy the broker map and each topic's partition map.
 3. return the copies as an FSM snapshot.

Returns:
  - an FSM snapshot containing copied state, and an error if snapshot creation fails.
*/
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

/*
Restore replaces the FSM state with data from a snapshot.

steps:
 1. decode the snapshot from the reader.
 2. initialize any missing broker or topic maps.
 3. lock the FSM and replace its state with the decoded data.

Returns:
  - an error if the snapshot cannot be decoded; otherwise nil.
*/
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

/*
Metadata returns copies of the broker and topic metadata.

steps:
 1. read-lock the FSM state.
 2. copy the broker map and each topic's partition map.
 3. return both copies.

Returns:
  - a copy of the broker map and a copy of the topic map.
*/
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

// fsmSnapshot contains a serialized copy of the FSM state.
type fsmSnapshot struct {
	Brokers map[int32]string       `json:"brokers"`
	Topics  map[string]*TopicState `json:"topics"`
}

/*
Persist writes the snapshot to the provided Raft snapshot sink.

steps:
 1. marshal the snapshot as JSON.
 2. write the encoded data to the sink.
 3. close the sink, or cancel it if an error occurred.

Returns:
  - an error if encoding, writing, or closing the snapshot fails; otherwise nil.
*/
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

/*
Release releases resources held by the snapshot.

steps:
 1. perform no action because this snapshot holds no external resources.

Returns:
  - nothing.
*/
func (s *fsmSnapshot) Release() {}

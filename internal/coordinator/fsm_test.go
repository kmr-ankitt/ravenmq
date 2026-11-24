package coordinator

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// mockSnapshotSink implements raft.SnapshotSink in memory for testing Snapshot().
type mockSnapshotSink struct {
	buf      bytes.Buffer
	closed   bool
	canceled bool
}

func (m *mockSnapshotSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *mockSnapshotSink) Close() error                { m.closed = true; return nil }
func (m *mockSnapshotSink) ID() string                  { return "mock-snap-1" }
func (m *mockSnapshotSink) Cancel() error               { m.canceled = true; return nil }

func mustApply(t *testing.T, fsm *FSM, cmd Command) any {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyDeterministicPartitionAssignment(t *testing.T) {
	fsm := NewFSM()

	// 1. Creating a topic with no registered brokers must return an error
	if res := mustApply(t, fsm, Command{
		Type:       CmdCreateTopic,
		Topic:      "orders",
		Partitions: 3,
	}); res == nil {
		t.Fatalf("expected error when creating topic with zero brokers, got nil")
	}

	// 2. Register brokers out of numerical order (30, 10, 20)
	// Sorted order must be [10, 20, 30]
	for _, b := range []struct {
		id   int32
		addr string
	}{
		{30, "127.0.0.1:9003"},
		{10, "127.0.0.1:9001"},
		{20, "127.0.0.1:9002"},
	} {
		if res := mustApply(t, fsm, Command{
			Type:     CmdRegisterBroker,
			BrokerID: b.id,
			GrpcAddr: b.addr,
		}); res != nil {
			t.Fatalf("unexpected error registering broker %d: %v", b.id, res)
		}
	}

	// 3. Create topic with 4 partitions -> expects [10, 20, 30, 10]
	if res := mustApply(t, fsm, Command{
		Type:       CmdCreateTopic,
		Topic:      "orders",
		Partitions: 4,
	}); res != nil {
		t.Fatalf("unexpected error creating topic: %v", res)
	}

	// 4. Duplicate CreateTopic must be idempotent (no error, no overwrite)
	if res := mustApply(t, fsm, Command{
		Type:       CmdCreateTopic,
		Topic:      "orders",
		Partitions: 99,
	}); res != nil {
		t.Fatalf("expected idempotent nil on duplicate topic, got %v", res)
	}

	brokers, topics := fsm.Metadata()
	if len(brokers) != 3 {
		t.Fatalf("expected 3 brokers, got %d", len(brokers))
	}

	orders := topics["orders"]
	if orders == nil || len(orders.Partitions) != 4 {
		t.Fatalf("expected 4 partitions for orders, got %+v", orders)
	}

	want := map[int32]int32{0: 10, 1: 20, 2: 30, 3: 10}
	for p, expectedLeader := range want {
		if got := orders.Partitions[p]; got != expectedLeader {
			t.Fatalf("partition %d: got leader %d, want %d", p, got, expectedLeader)
		}
	}
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	src := NewFSM()
	mustApply(t, src, Command{Type: CmdRegisterBroker, BrokerID: 1, GrpcAddr: "127.0.0.1:9001"})
	mustApply(t, src, Command{Type: CmdRegisterBroker, BrokerID: 2, GrpcAddr: "127.0.0.1:9002"})
	mustApply(t, src, Command{Type: CmdCreateTopic, Topic: "events", Partitions: 2})

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	if !sink.closed || sink.canceled {
		t.Fatalf("expected sink to be closed and not canceled")
	}

	// Restore into a fresh FSM and verify identical state
	dst := NewFSM()
	if err := dst.Restore(io.NopCloser(&sink.buf)); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	brokers, topics := dst.Metadata()
	if len(brokers) != 2 || brokers[1] != "127.0.0.1:9001" || brokers[2] != "127.0.0.1:9002" {
		t.Fatalf("restored brokers mismatch: %+v", brokers)
	}
	if ev := topics["events"]; ev == nil || ev.Partitions[0] != 1 || ev.Partitions[1] != 2 {
		t.Fatalf("restored topics mismatch: %+v", topics)
	}
}

func TestFSM_InvalidLogData(t *testing.T) {
	fsm := NewFSM()

	if res := fsm.Apply(&raft.Log{Data: []byte("{bad-json")}); res == nil {
		t.Fatalf("expected error for malformed JSON")
	}
	if res := mustApply(t, fsm, Command{Type: "unknown_op"}); res == nil {
		t.Fatalf("expected error for unknown command type")
	}
}
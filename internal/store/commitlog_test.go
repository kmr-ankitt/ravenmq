package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestCommitLog_AppendReadAndIdempotency(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "0.log")

	cl, err := NewCommitLog(logPath)
	if err != nil {
		t.Fatalf("NewCommitLog failed: %v", err)
	}
	defer cl.Close()

	msg1 := []byte("first event")
	msg2 := []byte("second event")

	// 1. First append starts at byte 0
	off1, err := cl.AppendIdempotent(1, 1, msg1)
	if err != nil {
		t.Fatalf("append 1 failed: %v", err)
	}
	if off1 != 0 {
		t.Fatalf("expected offset 0, got %d", off1)
	}
	if err := cl.SetCommitLogState(1, 1, 1); err != nil {
		t.Fatalf("SetCommitLogState failed: %v", err)
	}

	// 2. Retrying same (producerID=1, seq=1) must return -1
	dupOff, err := cl.AppendIdempotent(1, 1, msg1)
	if err != nil {
		t.Fatalf("duplicate append errored: %v", err)
	}
	if dupOff != -1 {
		t.Fatalf("expected -1 for duplicate message, got %d", dupOff)
	}

	// 3. Second append starts right after [8-byte header + len(msg1)]
	expectedOff2 := int64(lenWidth + len(msg1))
	off2, err := cl.AppendIdempotent(1, 2, msg2)
	if err != nil {
		t.Fatalf("append 2 failed: %v", err)
	}
	if off2 != expectedOff2 {
		t.Fatalf("expected offset %d, got %d", expectedOff2, off2)
	}
	if err := cl.SetCommitLogState(2, 1, 2); err != nil {
		t.Fatalf("SetCommitLogState failed: %v", err)
	}

	// 4. Sequential reads return exact payload and next byte offset
	got1, next1, err := cl.Read(0)
	if err != nil {
		t.Fatalf("read 1 failed: %v", err)
	}
	if string(got1) != string(msg1) || next1 != expectedOff2 {
		t.Fatalf("read 1 mismatch: got %q (next %d), want %q (next %d)", got1, next1, msg1, expectedOff2)
	}

	got2, next2, err := cl.Read(next1)
	if err != nil {
		t.Fatalf("read 2 failed: %v", err)
	}
	if string(got2) != string(msg2) {
		t.Fatalf("read 2 mismatch: got %q, want %q", got2, msg2)
	}

	// 5. Reading at EOF returns wrapped ErrOffsetOutOfBounds
	if _, _, err := cl.Read(next2); !errors.Is(err, ErrOffsetOutOfBounds) {
		t.Fatalf("expected ErrOffsetOutOfBounds at EOF, got %v", err)
	}
}

func TestCommitLog_PersistenceAcrossRestart(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "0.log")

	cl, err := NewCommitLog(logPath)
	if err != nil {
		t.Fatalf("NewCommitLog failed: %v", err)
	}

	if _, err := cl.AppendIdempotent(42, 10, []byte("persisted")); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := cl.SetCommitLogState(99, 42, 10); err != nil {
		t.Fatalf("SetCommitLogState failed: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Reopen from disk
	reopened, err := NewCommitLog(logPath)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if got := reopened.GetLastAppliedIndex(); got != 99 {
		t.Fatalf("expected LastAppliedIndex 99, got %d", got)
	}

	// Sequence 10 for producer 42 must still be rejected after restart
	dupOff, err := reopened.AppendIdempotent(42, 10, []byte("persisted"))
	if err != nil || dupOff != -1 {
		t.Fatalf("expected duplicate to be skipped after restart, got off=%d err=%v", dupOff, err)
	}

	data, _, err := reopened.Read(0)
	if err != nil || string(data) != "persisted" {
		t.Fatalf("expected 'persisted', got %q (err=%v)", data, err)
	}
}

func TestCommitLog_ConcurrentReads(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "0.log")
	cl, err := NewCommitLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	var offsets []int64
	for i := uint64(1); i <= 25; i++ {
		off, err := cl.AppendIdempotent(1, i, []byte("concurrent-record"))
		if err != nil {
			t.Fatal(err)
		}
		if off == -1 {
			t.Fatalf("unexpected duplicate at sequence %d", i)
		}
		if err := cl.SetCommitLogState(i, 1, i); err != nil {
			t.Fatalf("SetCommitLogState failed: %v", err)
		}
		offsets = append(offsets, off)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, off := range offsets {
				rec, _, err := cl.Read(off)
				if err != nil || string(rec) != "concurrent-record" {
					t.Errorf("concurrent read failed at offset %d: %v", off, err)
				}
			}
		}()
	}
	wg.Wait()
}

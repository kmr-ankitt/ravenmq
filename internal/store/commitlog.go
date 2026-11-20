package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

const lenWidth = 8

var ErrOffsetOutOfBounds = errors.New("offset out of bounds")

/*
*

	commitLogState represents the durable state of the commit log.
	Fields:
	- LastAppliedIndex: The index of the last applied message in the commit log.
	- ProducerLastSequence: The last sequence number produced by the producer.

*
*/
type commitLogState struct {
	LastAppliedIndex     uint64            `json:"last_applied_index"`
	ProducerLastSequence map[uint64]uint64 `json:"producer_last_sequence"`
}

/*
*

	A CommitLog is a persistent log of committed messages.
	It provides a way to store and retrieve committed messages in a durable manner.

	Fields:
	- mu: A mutex to synchronize access to the commit log.
	- file: The underlying file that stores the commit log.
	- size: The current size of the commit log file.
	- state: The durable state of the commit log, including the last applied index and producer's last sequence number.
	- statePath: The path to the file that stores the commit log's state.

*
*/
type CommitLog struct {
	mu        sync.RWMutex
	file      *os.File
	size      int64
	state     commitLogState
	statePath string
}

// NewCommitLog creates a new CommitLog instance and initializes it by loading the state from the .state file.
func NewCommitLog(path string) (*CommitLog, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open commit log file: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat commit log file: %w", err)
	}

	c := &CommitLog{
		file:      file,
		size:      fi.Size(),
		statePath: path + ".state",
	}

	if err := c.LoadState(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load commit log state: %w", err)
	}

	return c, nil
}

// loadState reads the .state file from disk.
// if state file doesn't exist, start with a fresh state.
func (c *CommitLog) LoadState() error {
	data, err := os.ReadFile(c.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.state = commitLogState{LastAppliedIndex: 0, ProducerLastSequence: make(map[uint64]uint64)}
			return nil
		}
		return err
	}

	if err := json.Unmarshal(data, &c.state); err != nil {
		return err
	}
	if c.state.ProducerLastSequence == nil {
		c.state.ProducerLastSequence = make(map[uint64]uint64)
	}

	return nil
}

// persiststate writes the current state of the commit log to the .state file.
func (c *CommitLog) PersistState() error {
	data, err := json.Marshal(c.state)
	if err != nil {
		return err
	}

	return os.WriteFile(c.statePath, data, 0644)
}

/*
AppendIdempotent appends a message to the commit log in an idempotent manner.
it checks the producer's last sequence number to ensure that duplicate messages are not appended.
if the message is a duplicate, it returns -1 and does not append the message,
else it appends the message to the commit log and returns the position of the appended message.
*/
func (c *CommitLog) AppendIdempotent(producerID uint64, sequence uint64, message []byte) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	lastSeq, ok := c.state.ProducerLastSequence[producerID]
	if ok && sequence <= lastSeq {
		fmt.Println("duplicate was skipped")
		return -1, nil
	}

	pos := c.size
	var lenBuf [lenWidth]byte

	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(message)))
	if _, err := c.file.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("failed to write record length: %w", err)
	}

	if _, err := c.file.Write(message); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	c.size += int64(lenWidth + len(message))

	return pos, nil
}

// Read reads a message from the commit log at the specified offset.
// it returns the message, the next offset, and an error if any.
func (c *CommitLog) Read(offset int64) ([]byte, int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if offset < 0 || offset >= c.size {
		return nil, 0, fmt.Errorf("offset %d: %w", offset, ErrOffsetOutOfBounds)
	}

	// read the length of the record at the specified offset
	lenBuf := make([]byte, lenWidth)
	if _, err := c.file.ReadAt(lenBuf, offset); err != nil {
		return nil, 0, fmt.Errorf("failed to read record length: %w", err)
	}

	recordLen := binary.BigEndian.Uint64(lenBuf)

	// read the actual record data based on the length read
	data := make([]byte, recordLen)
	if _, err := c.file.ReadAt(data, offset+lenWidth); err != nil {
		return nil, 0, fmt.Errorf("failed to read record: %w", err)
	}

	nextOffset := offset + lenWidth + int64(recordLen)

	return data, nextOffset, nil
}

func (c *CommitLog) GetLastAppliedIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.state.LastAppliedIndex
}

func (c *CommitLog) SetCommitLogState(index uint64, producerID uint64, sequence uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.LastAppliedIndex = index
	c.state.ProducerLastSequence[producerID] = sequence

	return c.PersistState()
}

func (c *CommitLog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.file.Close(); err != nil {
		return fmt.Errorf("failed to close commit log file: %w", err)
	}

	return nil
}

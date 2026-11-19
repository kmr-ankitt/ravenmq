package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

const lenWidth = 8

var ErrOffsetOutOfBounds = errors.New("offset out of bounds")

/**
 commitLogState represents the durable state of the commit log.
 Fields:
 - LastAppliedIndex: The index of the last applied message in the commit log.
 - ProducerLastSequence: The last sequence number produced by the producer.
**/
type commitLogState struct {
	LastAppliedIndex     uint64 `json:"last_applied_index"`
	ProducerLastSequence map[uint64]uint64 `json:"producer_last_sequence"`
}

/**
 A CommitLog is a persistent log of committed messages.
 It provides a way to store and retrieve committed messages in a durable manner.

 Fields:
 - mu: A mutex to synchronize access to the commit log.
 - file: The underlying file that stores the commit log.
 - size: The current size of the commit log file.
 - state: The durable state of the commit log, including the last applied index and producer's last sequence number.
 - statePath: The path to the file that stores the commit log's state.
**/
type CommitLog struct {
	mu        sync.RWMutex
	file      *os.File
	size      int64
	state     commitLogState
	statePath string
}

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

	if err := c.loadState(); err != nil {
		return nil, fmt.Errorf("failed to load commit log state: %w", err)
	}

	return c, nil
}


func newCommitLog(path string) (*CommitLog, error) {

}

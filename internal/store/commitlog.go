package store

import (
	"errors"
	"os"
	"sync"
)

const lenWidth = 8

var ErrOffsetOutOfBounds = errors.New("offset out of bounds")

// commitLogState holds the durable state for a commit log.
type commitLogState struct {
	LastAppliedIndex     uint64 `json:"last_applied_index"`
	ProducerLastSequence uint64 `json:"producer_last_sequence"`
}

type CommitLog struct {
	mu sync.RWMutex
	file *os.File
	size int64
	state commitLogState
	statePath string
}

func newCommitLog(path string) (*CommitLog, error) {

}

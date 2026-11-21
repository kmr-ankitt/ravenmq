package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"github.com/kmr-ankitt/ravenmq/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
Server represents the broker server.

  - ravenmqv1.UnimplementedBrokerServiceServer: Provides default gRPC methods.
  - mu: Protects shared server state.
  - dataDir: Stores the data directory path.
  - logs: Maps topics to their commit logs.
*/
type Server struct {
	ravenmqv1.UnimplementedBrokerServiceServer
	mu      sync.RWMutex
	dataDir string
	logs    map[string]*store.CommitLog
}

func NewServer(dataDir string) (*Server, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return &Server{
		dataDir: dataDir,
		logs:    make(map[string]*store.CommitLog),
	}, nil
}

/*
getOrCreateLog retrieves an existing commit log for a given topic and partition,
or creates a new one if it doesn't exist.

Parameters:
  - topic: The topic name.
  - partition: The partition number.
*/
func (s *Server) getOrCreateLog(topic string, partition int32) (*store.CommitLog, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	// check if the log already exists in memory
	s.mu.RLock()
	log, exists := s.logs[key]
	s.mu.RUnlock()

	if exists {
		return log, nil
	}

	// if the log doesn't exist, create it
	s.mu.Lock()
	defer s.mu.Unlock()

	// double-check if the log was created by another goroutine while acquiring the write lock
	if log, exists = s.logs[key]; exists {
		return log, nil
	}

	topicDir := filepath.Join(s.dataDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}

	logPath := filepath.Join(topicDir, fmt.Sprintf("%d.log", partition))
	log, err := store.NewCommitLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create commit log: %w", err)
	}

	s.logs[key] = log
	return log, nil
}

/*
Produce handles a produce grpc request.

steps:
 1. get or create the commit log for the requested topic and partition.
 2. append the message idempotently using the producer id and sequence number.
 3. persist the commit log state when a new message was appended.
 4. return the message offset and a success error code.

Returns:
  - ProduceResponse containing the offset of the appended message and an error code.
*/
func (s *Server) Produce(ctx context.Context, req *ravenmqv1.ProduceRequest) (*ravenmqv1.ProduceResponse, error) {
	if req.Topic == "" || req.Partition < 0 {
		return nil, status.Error(codes.InvalidArgument, "topic must be non-empty and partition must be whole number")
	}

	log, err := s.getOrCreateLog(req.GetTopic(), req.GetPartition())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get log: %v", err)
	}

	offset, err := log.AppendIdempotent(req.GetProducerId(), req.GetSequenceNumber(), req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append idempotent failed: %v", err)
	}

	if offset != -1 {
		if err := log.SetCommitLogState(uint64(offset), req.GetProducerId(), req.GetSequenceNumber()); err != nil {
			return nil, status.Errorf(codes.Internal, "persist commit log state: %v", err)
		}
	}

	return &ravenmqv1.ProduceResponse{
		Offset:    offset,
		ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_NONE,
	}, nil
}

/*
Consume handles a consume grpc request.

steps:
 1. validate the topic and partition.
 2. get or create the commit log for the requested topic and partition.
 3. read the message at the requested offset.
 4. return the consumed record and the next offset.

Returns:
  - ConsumeResponse containing the record and an error code.
*/
func (s *Server) Consume(ctx context.Context, req *ravenmqv1.ConsumeRequest) (*ravenmqv1.ConsumeResponse, error) {
	if req.GetTopic() == "" || req.GetPartition() < 0 || req.GetOffset() < 0 {
		return nil, status.Error(codes.InvalidArgument, "topic should be non empty and partition, offset should be whole number")
	}

	log, err := s.getOrCreateLog(req.GetTopic(), req.GetPartition())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get log: %v", err)
	}

	data, nextOffset, err := log.Read(req.GetOffset())
	if err != nil {
		if errors.Is(err, store.ErrOffsetOutOfBounds) {
			return &ravenmqv1.ConsumeResponse{
				ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_BOUNDS,
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "read record: %v", err)
	}

	return &ravenmqv1.ConsumeResponse{
		Record: &ravenmqv1.Record{
			Value:  data,
			Offset: nextOffset,
		},
		ErrorCode: ravenmqv1.ErrorCode_ERROR_CODE_NONE,
	}, nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for key, log := range s.logs {
		if err := log.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

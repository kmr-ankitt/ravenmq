package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config contains the local network addresses and storage settings for a coordinator node.
type Config struct {
	NodeID    string
	RaftAddr  string
	GrpcAddr  string
	DataDir   string
	Bootstrap bool
}

// Server implements the coordinator gRPC service and Raft-backed cluster state.
type Server struct {
	ravenmqv1.UnimplementedCoordinatorServiceServer
	raft      *raft.Raft
	fsm       *FSM
	config    Config
	boltStore *raftboltdb.BoltStore
	transport *raft.NetworkTransport
}

/*
NewServer creates and starts a coordinator server.

steps:
 1. create the coordinator data directory and persistent Raft stores.
 2. configure the Raft TCP transport and finite state machine.
 3. start the Raft node and optionally bootstrap a single-node cluster.

Returns:
  - Server containing the running Raft node and coordinator state.
  - Error if directory, store, transport, or Raft initialization fails.
*/
func NewServer(cfg Config) (*Server, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create coordinator data dir: %w", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft tcp transport: %w", err)
	}

	fsm := NewFSM()
	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = transport.Close()
		_ = boltStore.Close()
		return nil, fmt.Errorf("start raft node: %w", err)
	}

	if cfg.Bootstrap {
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftCfg.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := r.BootstrapCluster(bootstrapCfg).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			_ = r.Shutdown().Error()
			_ = transport.Close()
			_ = boltStore.Close()
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return &Server{
		raft:      r,
		fsm:       fsm,
		config:    cfg,
		boltStore: boltStore,
		transport: transport,
	}, nil
}

/*
applyCommand marshals and applies a command to the Raft log.

steps:
 1. marshal the command as JSON.
 2. submit the command to the Raft state machine.
 3. wait for the application future and return any response error.

Returns:
  - an error if marshalling or applying the command fails.
*/
func (s *Server) applyCommand(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := s.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	if respErr, ok := future.Response().(error); ok && respErr != nil {
		return respErr
	}
	return nil
}

// JoinCluster adds a Raft voter to the cluster when this server is the leader.
func (s *Server) JoinCluster(ctx context.Context, req *ravenmqv1.JoinClusterRequest) (*ravenmqv1.JoinClusterResponse, error) {
	if req.GetNodeId() == "" || req.GetRaftAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and raft_addr are required")
	}

	if s.raft.State() != raft.Leader {
		return &ravenmqv1.JoinClusterResponse{
			Success:       false,
			LeaderAddress: string(s.raft.Leader()),
		}, nil
	}

	future := s.raft.AddVoter(
		raft.ServerID(req.GetNodeId()),
		raft.ServerAddress(req.GetRaftAddr()),
		0,
		0,
	)
	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "add voter: %v", err)
	}

	return &ravenmqv1.JoinClusterResponse{
		Success:       true,
		LeaderAddress: string(s.raft.Leader()),
	}, nil
}

/*
GetClusterMetadata returns the current cluster metadata from the FSM.

steps:
 1. lock the FSM state for reading.
 2. copy the broker and topic metadata into response structures.
 3. return the cluster metadata.

Returns:
  - GetClusterMetadataResponse containing brokers and topics.
*/
func (s *Server) RegisterBroker(ctx context.Context, req *ravenmqv1.RegisterBrokerRequest) (*ravenmqv1.RegisterBrokerResponse, error) {
	if req.GetBrokerId() <= 0 || req.GetGrpcAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "broker_id > 0 and grpc_addr are required")
	}

	if s.raft.State() != raft.Leader {
		return &ravenmqv1.RegisterBrokerResponse{
			Success:       false,
			LeaderAddress: string(s.raft.Leader()),
		}, nil
	}

	err := s.applyCommand(Command{
		Type:     CmdRegisterBroker,
		BrokerID: req.GetBrokerId(),
		GrpcAddr: req.GetGrpcAddr(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register broker: %v", err)
	}

	return &ravenmqv1.RegisterBrokerResponse{
		Success:       true,
		LeaderAddress: string(s.raft.Leader()),
	}, nil
}

/*
CreateTopic handles a create topic grpc request.

steps:
 1. validate the topic name and partition count.
 2. check if this server is the Raft leader; if not, return the leader address.
 3. apply a create topic command to the Raft log.
 4. return a success response with the leader address.
 
Returns:
  - CreateTopicResponse containing an error code and the leader address.
*/
func (s *Server) CreateTopic(ctx context.Context, req *ravenmqv1.CreateTopicRequest) (*ravenmqv1.CreateTopicResponse, error) {
	if req.GetTopic() == "" || req.GetPartitions() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "topic must be non-empty and partitions > 0")
	}

	if s.raft.State() != raft.Leader {
		return &ravenmqv1.CreateTopicResponse{
			ErrorCode:     ravenmqv1.ErrorCode_ERROR_CODE_NOT_LEADER,
			LeaderAddress: string(s.raft.Leader()),
		}, nil
	}

	err := s.applyCommand(Command{
		Type:       CmdCreateTopic,
		Topic:      req.GetTopic(),
		Partitions: req.GetPartitions(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create topic: %v", err)
	}

	return &ravenmqv1.CreateTopicResponse{
		ErrorCode:     ravenmqv1.ErrorCode_ERROR_CODE_NONE,
		LeaderAddress: string(s.raft.Leader()),
	}, nil
}

/*
GetClusterMetadata returns the current cluster metadata from the FSM.

steps:
 1. retrieve the broker and topic metadata from the FSM.
 2. convert the metadata into gRPC response structures.
 3. return the cluster metadata response.

Returns:
  - GetClusterMetadataResponse containing brokers and topics.
*/
func (s *Server) GetClusterMetadata(ctx context.Context, req *ravenmqv1.GetClusterMetadataRequest) (*ravenmqv1.GetClusterMetadataResponse, error) {
	brokersMap, topicsMap := s.fsm.Metadata()

	brokers := make([]*ravenmqv1.BrokerInfo, 0, len(brokersMap))
	for id, addr := range brokersMap {
		brokers = append(brokers, &ravenmqv1.BrokerInfo{
			BrokerId: id,
			GrpcAddr: addr,
		})
	}

	topics := make([]*ravenmqv1.TopicMetadata, 0, len(topicsMap))
	for _, t := range topicsMap {
		partitions := make([]*ravenmqv1.PartitionInfo, 0, len(t.Partitions))
		for pID, leaderID := range t.Partitions {
			partitions = append(partitions, &ravenmqv1.PartitionInfo{
				PartitionId:    pID,
				LeaderBrokerId: leaderID,
			})
		}
		topics = append(topics, &ravenmqv1.TopicMetadata{
			Topic:      t.Name,
			Partitions: partitions,
		})
	}

	return &ravenmqv1.GetClusterMetadataResponse{
		Brokers: brokers,
		Topics:  topics,
	}, nil
}

func (s *Server) Close() error {
	var errs []error
	if err := s.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("shutdown raft: %w", err))
	}
	if err := s.transport.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close transport: %w", err))
	}
	if err := s.boltStore.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close bolt store: %w", err))
	}
	return errors.Join(errs...)
}

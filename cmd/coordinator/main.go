package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"github.com/kmr-ankitt/ravenmq/internal/coordinator"
	"github.com/kmr-ankitt/ravenmq/internal/groupcoord"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	nodeID := flag.String("node-id", "coord-1", "unique Raft node ID")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7001", "Raft consensus TCP address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:8001", "Coordinator gRPC listen address")
	dataDir := flag.String("data-dir", "./data/coord-1", "directory for Raft BoltDB and snapshots")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap a new single-node Raft cluster")
	joinAddr := flag.String("join", "", "gRPC address of an existing Coordinator leader to join")
	flag.Parse()

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *grpcAddr, err)
	}

	srv, err := coordinator.NewServer(coordinator.Config{
		NodeID:    *nodeID,
		RaftAddr:  *raftAddr,
		GrpcAddr:  *grpcAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
	})
	if err != nil {
		log.Fatalf("failed to initialize coordinator: %v", err)
	}

	groupSrv := groupcoord.NewServer()

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterCoordinatorServiceServer(grpcServer, srv)
	ravenmqv1.RegisterGroupCoordinatorServiceServer(grpcServer, groupSrv)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("coordinator %s listening (grpc=%s, raft=%s)", *nodeID, *grpcAddr, *raftAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	if *joinAddr != "" {
		if err := joinCluster(*joinAddr, *nodeID, *raftAddr, *grpcAddr); err != nil {
			log.Fatalf("failed to join cluster at %s: %v", *joinAddr, err)
		}
		log.Printf("joined cluster via %s", *joinAddr)
	}

	<-ctx.Done()
	log.Println("shutting down coordinator...")
	grpcServer.GracefulStop()
	if err := srv.Close(); err != nil {
		log.Printf("error closing coordinator: %v", err)
	}
	log.Println("coordinator stopped")
}

/*
joinCluster dials an existing Coordinator seed node and requests voter membership in the Raft cluster.

steps:
1. open an insecure gRPC client connection to the seed node address.
2. send a JoinCluster RPC with this node's ID, Raft address, and gRPC address.
3. log a warning if the seed node is not the current Raft leader.

Returns:
- an error if dialing or the RPC fails, or nil on success.
*/
func joinCluster(seedAddr, nodeID, raftAddr, grpcAddr string) error {
	conn, err := grpc.NewClient(seedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := ravenmqv1.NewCoordinatorServiceClient(conn)
	reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.JoinCluster(reqCtx, &ravenmqv1.JoinClusterRequest{
		NodeId:   nodeID,
		RaftAddr: raftAddr,
		GrpcAddr: grpcAddr,
	})
	if err != nil {
		return err
	}
	if !resp.GetSuccess() {
		log.Printf("seed node was not leader; retry against leader at %s", resp.GetLeaderAddress())
	}
	return nil
}

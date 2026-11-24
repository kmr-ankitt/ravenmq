package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"github.com/kmr-ankitt/ravenmq/internal/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	brokerID := flag.Int("broker-id", 1, "unique numeric ID for this broker")
	addr := flag.String("addr", "127.0.0.1:9001", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data/broker-1", "directory for commit logs")
	coordAddr := flag.String("coordinator", "", "Coordinator gRPC address for self-registration")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	srv, err := broker.NewServer(*dataDir)
	if err != nil {
		log.Fatalf("failed to initialize broker server: %v", err)
	}

	grpcServer := grpc.NewServer()
	ravenmqv1.RegisterBrokerServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("broker %d listening on %s (data-dir=%s)", *brokerID, *addr, *dataDir)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	if *coordAddr != "" {
		if err := registerWithCoordinator(*coordAddr, int32(*brokerID), *addr); err != nil {
			log.Fatalf("failed to register broker with coordinator: %v", err)
		}
		log.Printf("registered broker %d (%s) with coordinator", *brokerID, *addr)
	}

	<-ctx.Done()
	log.Println("shutting down broker...")
	grpcServer.GracefulStop()
	if err := srv.Close(); err != nil {
		log.Printf("error closing broker logs: %v", err)
	}
	log.Println("broker stopped")
}

/*
registerWithCoordinator announces this broker to the Coordinator cluster with retries during leader election.

steps:
1. open an insecure gRPC client connection to the target Coordinator address.
2. attempt RegisterBroker up to 5 times, retrying if the cluster is still electing a leader.
3. return nil once registration succeeds, or an error if all attempts fail.

Returns:
- an error if connection or registration fails after retries, or nil on success.
*/
func registerWithCoordinator(coordAddr string, brokerID int32, grpcAddr string) error {
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial coordinator %s: %w", coordAddr, err)
	}
	defer conn.Close()

	client := ravenmqv1.NewCoordinatorServiceClient(conn)

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		reqCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.RegisterBroker(reqCtx, &ravenmqv1.RegisterBrokerRequest{
			BrokerId: brokerID,
			GrpcAddr: grpcAddr,
		})
		cancel()

		if err == nil && resp.GetSuccess() {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("coordinator at %s is not leader (leader=%s)", coordAddr, resp.GetLeaderAddress())
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("register broker after retries: %w", lastErr)
}

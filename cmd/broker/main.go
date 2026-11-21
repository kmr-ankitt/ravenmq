package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	ravenmqv1 "github.com/kmr-ankitt/ravenmq/api/proto/v1"
	"github.com/kmr-ankitt/ravenmq/internal/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	addr := flag.String("addr", ":9001", "gRPC listen address")
	dataDir := flag.String("data-dir", "./data/broker-1", "directory for commit logs")
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
		log.Printf("broker listening on %s (data-dir=%s)", *addr, *dataDir)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down broker...")
	grpcServer.GracefulStop()
	if err := srv.Close(); err != nil {
		log.Printf("error closing broker logs: %v", err)
	}
	log.Println("broker stopped")
}

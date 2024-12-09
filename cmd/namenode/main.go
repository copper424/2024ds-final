package main

import (
	"log"
	pb "my_rpc"
	"net"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", "localhost:20241")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	namenode := NewNameNodeServer()
	pb.RegisterNameNodeServiceServer(server, namenode)

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

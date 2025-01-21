package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	pb "my_rpc"
	"net"
	"os"

	"google.golang.org/grpc"
)

type SecondaryNameNodeServer struct {
	pb.UnimplementedSecondaryNameNodeServiceServer
	metadata map[string]*pb.FileMetadata
	filePath string
}

func (s *SecondaryNameNodeServer) ReceiveMetadata(ctx context.Context, req *pb.MetadataPayload) (*pb.StatusResponse, error) {
	if err := json.Unmarshal(req.Data, &s.metadata); err != nil {
		return &pb.StatusResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	// log.Default().Printf("Received metadata: %v", s.metadata)
	// Persist to disk
	data, _ := json.Marshal(s.metadata)
	_ = os.WriteFile(s.filePath, data, 0644)
	return &pb.StatusResponse{Success: true}, nil
}

func (s *SecondaryNameNodeServer) RestoreMetadata(ctx context.Context, _ *pb.Empty) (*pb.MetadataPayload, error) {
	if data, err := os.ReadFile(s.filePath); err == nil {
		json.Unmarshal(data, &s.metadata)
		return &pb.MetadataPayload{Data: data}, nil
	}
	return &pb.MetadataPayload{}, fmt.Errorf("no backup found")
}

func main() {
	server := grpc.NewServer()
	srv := &SecondaryNameNodeServer{
		metadata: make(map[string]*pb.FileMetadata),
		filePath: "./secondary_backup.json",
	}
	// Optionally load existing backup
	if data, err := os.ReadFile(srv.filePath); err == nil {
		json.Unmarshal(data, &srv.metadata)
	}

	pb.RegisterSecondaryNameNodeServiceServer(server, srv)

	lis, err := net.Listen("tcp", "localhost:20242")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

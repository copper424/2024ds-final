package main

import (
	"context"
	"encoding/json"
	pb "my_rpc"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *NameNodeServer) BackupMetadata(filePath string) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

func (s *NameNodeServer) RestoreMetadata(filePath string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.metadata)
}

func (s *NameNodeServer) SendMetadataToSecondary(ctx context.Context, secondary_namenode string) (*pb.StatusResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	conn, err := grpc.NewClient(secondary_namenode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.StatusResponse{Success: false}, err
	}
	defer conn.Close()
	data, err := json.Marshal(s.metadata)
	if err != nil {
		return &pb.StatusResponse{Success: false}, err
	}
	client := pb.NewSecondaryNameNodeServiceClient(conn)
	_, err = client.ReceiveMetadata(ctx, &pb.MetadataPayload{Data: data})
	if err != nil {
		return &pb.StatusResponse{Success: false}, err
	}
	return &pb.StatusResponse{Success: true}, nil
}

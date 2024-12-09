package main

import (
	"context"
	"fmt"
	pb "my_rpc"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DataNodeServer implements the DataNode service
type DataNodeServer struct {
	pb.UnimplementedDataNodeServiceServer // Required for forward compatibility with gRPC

	// Configuration
	dataDir string // Directory to store files

	// Internal state
	nodeID string // Unique identifier for this datanode
	host   string // Host address
	port   int32  // Port number
}

// NewDataNodeServer creates a new DataNode server instance
func NewDataNodeServer(dataDir, host string, port int32) (*DataNodeServer, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	return &DataNodeServer{
		dataDir: dataDir,
		host:    host,
		port:    port,
		nodeID:  fmt.Sprintf("%s:%d", host, port),
	}, nil
}

// getFilePath returns the full path where a file should be stored
func (s *DataNodeServer) getFilePath(path string) string {
	// Use hash or other method to distribute files across directories if needed
	return filepath.Join(s.dataDir, filepath.Clean(path))
}

// StoreFile implements the StoreFile RPC method
func (s *DataNodeServer) StoreFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	if req.Path == "" {
		return &pb.WriteFileResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	filePath := s.getFilePath(req.GetPath())

	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return &pb.WriteFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to create directories: %v", err),
		}, nil
	}

	// Write file with exclusive flag (fail if exists)
	err := os.WriteFile(filePath, req.Content, 0644)
	if err != nil {
		return &pb.WriteFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to write file: %v", err),
		}, nil
	}

	return &pb.WriteFileResponse{
		Success: true,
	}, nil
}

// RetrieveFile implements the RetrieveFile RPC method
func (s *DataNodeServer) RetrieveFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	if req.Path == "" {
		return &pb.ReadFileResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	filePath := s.getFilePath(req.GetPath())

	// Read file contents
	content, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.ReadFileResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("file not found: %s", req.Path),
			}, nil
		}
		return &pb.ReadFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to read file: %v", err),
		}, nil
	}

	return &pb.ReadFileResponse{
		Success: true,
		Content: content,
		Metadata: &pb.FileMetadata{
			Path: req.Path,
			Size: int64(len(content)),
			// Other metadata fields would be filled by the NameNode
		},
	}, nil
}

// DeleteFile implements the DeleteFile RPC method
func (s *DataNodeServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if req.Path == "" {
		return &pb.DeleteFileResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	filePath := s.getFilePath(req.GetPath())

	// Delete the file
	err := os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.DeleteFileResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("file not found: %s", req.Path),
			}, nil
		}
		return &pb.DeleteFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to delete file: %v", err),
		}, nil
	}

	return &pb.DeleteFileResponse{
		Success: true,
	}, nil
}

// StartDataNode starts the DataNode service and registers with the NameNode
func (s *DataNodeServer) StartDataNode(namenodeAddr string) error {
	// Connect to the NameNode
	conn, err := grpc.NewClient(namenodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	// Create NameNode client
	namenodeClient := pb.NewNameNodeServiceClient(conn)

	// Register with NameNode
	location := &pb.DatanodeLocation{
		Host: s.host,
		Port: s.port,
	}

	resp, err := namenodeClient.RegisterDataNode(context.Background(), location)
	if err != nil {
		return fmt.Errorf("failed to register with NameNode: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.ErrorMessage)
	}

	// Store the assigned ID
	s.nodeID = resp.DatanodeId

	return nil
}

package main

import (
	"context"
	pb "my_rpc"
	"path/filepath"
	"time"
)

// ListDirectory implements the ListDirectory RPC method
func (s *NameNodeServer) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Validate request
	if req.Path == "" {
		return &pb.ListDirectoryResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	// Normalize path
	dirPath := filepath.Clean(req.Path)

	// Find all files in the directory
	var files []*pb.FileMetadata
	for path, metadata := range s.metadata {
		// Check if the file is in the requested directory
		if filepath.Dir(path) == dirPath {
			files = append(files, metadata)
		}
	}

	return &pb.ListDirectoryResponse{
		Success: true,
		Files:   files,
	}, nil
}

func (s *NameNodeServer) MakeDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	path := filepath.Clean(req.Path)

	// Check if directory already exists
	if _, exists := s.metadata[path]; exists {
		return &pb.ListDirectoryResponse{
			Success:      false,
			ErrorMessage: "directory already exists",
		}, nil
	}

	// Create directory metadata
	dirMetadata := &pb.FileMetadata{
		Filename:         filepath.Base(path),
		Path:             path,
		Size:             0,
		Permission:       0755, // Default directory permissions
		Owner:            req.GetOwner(),
		CreationTime:     time.Now().UnixNano(),
		ModificationTime: time.Now().UnixNano(),
	}

	s.metadata[path] = dirMetadata

	return &pb.ListDirectoryResponse{
		Success: true,
		Files:   []*pb.FileMetadata{dirMetadata},
	}, nil
}

func (s *NameNodeServer) RemoveDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	path := filepath.Clean(req.Path)

	// Check if directory exists
	if _, exists := s.metadata[path]; !exists {
		return &pb.ListDirectoryResponse{
			Success:      false,
			ErrorMessage: "directory not found",
		}, nil
	}

	// Check if directory is empty
	for filePath := range s.metadata {
		// if filepath.Dir(filePath) == path {
		// 	return &pb.ListDirectoryResponse{
		// 		Success:      false,
		// 		ErrorMessage: "directory not empty",
		// 	}, nil
		// }
		s.DeleteFile(ctx, &pb.DeleteFileRequest{Path: filePath})
	}

	// Remove directory metadata
	delete(s.metadata, path)

	return &pb.ListDirectoryResponse{Success: true}, nil
}

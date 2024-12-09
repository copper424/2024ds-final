package main

import (
	"context"
	"errors"
	"fmt"
	pb "my_rpc"
	"path/filepath"
	"sync"
	"time"
)

// NameNodeServer implements the NameNode service
type NameNodeServer struct {
	pb.UnimplementedNameNodeServiceServer // Required for forward compatibility with gRPC

	// Internal state
	metadata  map[string]*pb.FileMetadata     // path -> metadata
	datanodes map[string]*pb.DatanodeLocation // datanode_id -> location
	mutex     sync.RWMutex

	// Add file locking system
	// fileLocks map[string]*sync.RWMutex // path -> lock
	lockHandle map[string]*LockHandle // path -> list of lock holders
}

type LockHandle struct {
	mu      sync.Mutex
	writer  int8
	readers int32
}

// NewNameNodeServer creates a new NameNode server instance
func NewNameNodeServer() *NameNodeServer {
	return &NameNodeServer{
		metadata:  make(map[string]*pb.FileMetadata),
		datanodes: make(map[string]*pb.DatanodeLocation),
		// fileLocks: make(map[string]*sync.RWMutex),
		lockHandle: make(map[string]*LockHandle),
	}
}

// CreateFile implements the ClientService CreateFile RPC method
func (s *NameNodeServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	path := filepath.Clean(req.Path)

	// Check if file already exists
	if _, exists := s.metadata[path]; exists {
		return &pb.CreateFileResponse{
			Success:      false,
			ErrorMessage: "file already exists",
		}, nil
	}

	// Select datanodes for replication
	selectedNodes, err := s.selectDataNodesForReplication(int(req.GetReplicationFactor())) // Default replication factor
	if err != nil {
		return &pb.CreateFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to select datanodes: %v", err),
		}, nil
	}

	// Create file metadata
	metadata := &pb.FileMetadata{
		Filename:         filepath.Base(path),
		Path:             path,
		Size:             int64(len(req.Content)),
		Permission:       req.Permission,
		Owner:            "copper424", // Current user
		CreationTime:     time.Now().UnixNano(),
		ModificationTime: time.Now().UnixNano(),
		Version:          0,
		Replicas:         selectedNodes,
	}

	// Update metadata
	s.metadata[path] = metadata

	// Create lock handle
	s.lockHandle[path] = &LockHandle{
		mu:      sync.Mutex{},
		writer:  0,
		readers: 0,
	}

	return &pb.CreateFileResponse{
		Success:  true,
		Metadata: metadata,
	}, nil
}

// DeleteFile implements the ClientService DeleteFile RPC method
func (s *NameNodeServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	path := filepath.Clean(req.Path)
	_, exists := s.metadata[path]
	if !exists {
		return &pb.DeleteFileResponse{
			Success:      false,
			ErrorMessage: "file not found",
		}, nil
	}

	// Remove metadata
	delete(s.metadata, path)

	// Remove lock handle
	delete(s.lockHandle, path)

	return &pb.DeleteFileResponse{
		Success: true,
	}, nil
}

func (s *NameNodeServer) MoveFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	path := filepath.Clean(req.Path)
	metadata, exists := s.metadata[path]
	if !exists {
		return &pb.ReadFileResponse{
			Success:      false,
			ErrorMessage: "source file not found",
		}, nil
	}

	// Return metadata for the file that needs to be moved
	return &pb.ReadFileResponse{
		Success:  true,
		Metadata: metadata,
	}, nil
}

// Helper functions

func (s *NameNodeServer) selectDataNodesForReplication(replicationFactor int) ([]*pb.DatanodeLocation, error) {
	if len(s.datanodes) < replicationFactor {
		return nil, fmt.Errorf("not enough datanodes available")
	}

	selected := make([]*pb.DatanodeLocation, 0, replicationFactor)
	for _, node := range s.datanodes {
		selected = append(selected, node)
		if len(selected) == replicationFactor {
			break
		}
	}

	return selected, nil
}

func (s *NameNodeServer) LockFile(ctx context.Context, req *pb.LockFileRequest) (*pb.LockFileResponse, error) {
	path := filepath.Clean(req.Path)

	if lock, exists := s.lockHandle[path]; exists {
		lock.mu.Lock()
		defer lock.mu.Unlock()
		if req.LockType == pb.LockType_WRITE {
			if lock.writer == 1 || lock.readers > 0 {
				return &pb.LockFileResponse{Success: false, ErrorMessage: "file already locked"}, errors.New("file already locked")
			} else {
				lock.writer = 1
			}
		} else if req.LockType == pb.LockType_READ {
			if lock.writer == 1 {
				return &pb.LockFileResponse{Success: false, ErrorMessage: "file already locked"}, errors.New("file already locked")
			} else {
				lock.readers += 1
			}
		} else {
			return &pb.LockFileResponse{Success: false, ErrorMessage: "invalid lock type"}, errors.New("invalid lock type")
		}
	} else {
		return &pb.LockFileResponse{Success: false, ErrorMessage: "lock not found"}, errors.New("lock not found")
	}

	return &pb.LockFileResponse{Success: true}, nil
}

func (s *NameNodeServer) UnlockFile(ctx context.Context, req *pb.UnlockFileRequest) (*pb.UnlockFileResponse, error) {
	path := filepath.Clean(req.Path)

	if lock, exists := s.lockHandle[path]; exists {
		lock.mu.Lock()
		defer lock.mu.Unlock()
		if req.LockType == pb.LockType_WRITE {
			if lock.writer == 0 {
				return &pb.UnlockFileResponse{Success: false, ErrorMessage: "file not locked"}, errors.New("file not locked")
			} else {
				lock.writer = 0
			}
		} else if req.LockType == pb.LockType_READ {
			if lock.readers == 0 {
				return &pb.UnlockFileResponse{Success: false, ErrorMessage: "file not locked"}, errors.New("file not locked")
			} else {
				lock.readers -= 1
			}
		} else {
			return &pb.UnlockFileResponse{Success: false, ErrorMessage: "invalid lock type"}, errors.New("invalid lock type")
		}
	} else {
		return &pb.UnlockFileResponse{Success: false, ErrorMessage: "Lock not found"}, errors.New("lock not found")
	}

	return &pb.UnlockFileResponse{Success: true}, nil
}

// GetFileMetadata implements the GetFileMetadata RPC method
func (s *NameNodeServer) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Validate request
	if req.Path == "" {
		return &pb.GetFileMetadataResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	// Normalize path
	path := filepath.Clean(req.Path)

	// Look up metadata
	metadata, exists := s.metadata[path]
	if !exists {
		return &pb.GetFileMetadataResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("file not found: %s", path),
		}, nil
	}

	return &pb.GetFileMetadataResponse{
		Success:  true,
		Metadata: metadata,
	}, nil
}

// GetFileLocations implements the GetFileLocations RPC method
func (s *NameNodeServer) GetFileLocations(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Validate request
	if req.Path == "" {
		return &pb.GetFileMetadataResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	// Normalize path
	path := filepath.Clean(req.Path)

	// Look up metadata
	metadata, exists := s.metadata[path]
	if !exists {
		return &pb.GetFileMetadataResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("file not found: %s", path),
		}, nil
	}

	// Verify that the replicas are still available
	var activeReplicas []*pb.DatanodeLocation
	for _, replica := range metadata.Replicas {
		if _, exists := s.datanodes[replica.DatanodeId]; exists {
			activeReplicas = append(activeReplicas, replica)
		}
	}

	// Create a copy of metadata with only active replicas
	metadataWithActiveReplicas := &pb.FileMetadata{
		Filename:         metadata.Filename,
		Path:             metadata.Path,
		Size:             metadata.Size,
		Permission:       metadata.Permission,
		Owner:            metadata.Owner,
		CreationTime:     metadata.CreationTime,
		ModificationTime: metadata.ModificationTime,
		Replicas:         activeReplicas,
	}

	return &pb.GetFileMetadataResponse{
		Success:  true,
		Metadata: metadataWithActiveReplicas,
	}, nil
}

// RegisterDataNode implements the RegisterDataNode RPC method
func (s *NameNodeServer) RegisterDataNode(ctx context.Context, location *pb.DatanodeLocation) (*pb.RegisterResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Validate request
	if location.Host == "" || location.Port == 0 {
		return &pb.RegisterResponse{
			Success:      false,
			ErrorMessage: "invalid datanode location",
		}, nil
	}

	// Generate datanode ID if not provided
	if location.DatanodeId == "" {
		location.DatanodeId = generateDatanodeID(location.Host, location.Port)
	}

	// Store datanode information
	s.datanodes[location.DatanodeId] = location

	return &pb.RegisterResponse{
		Success:    true,
		DatanodeId: location.DatanodeId,
	}, nil
}

// ReportFileStatus implements the ReportFileStatus RPC method
func (s *NameNodeServer) ReportFileStatus(ctx context.Context, metadata *pb.FileMetadata) (*pb.StatusResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Validate request
	if metadata.Path == "" {
		return &pb.StatusResponse{
			Success:      false,
			ErrorMessage: "path cannot be empty",
		}, nil
	}

	// Update metadata
	path := filepath.Clean(metadata.Path)
	existingMetadata, exists := s.metadata[path]

	if !exists {
		// New file
		metadata.CreationTime = time.Now().UnixNano()
	} else {
		// Update existing file
		metadata.CreationTime = existingMetadata.CreationTime
	}

	metadata.ModificationTime = time.Now().UnixNano()
	s.metadata[path] = metadata

	return &pb.StatusResponse{
		Success: true,
	}, nil
}

// Helper function to generate a unique datanode ID
func generateDatanodeID(host string, port int32) string {
	return fmt.Sprintf("%s:%d", host, port)
}

package main

import (
	"context"
	"errors"
	"fmt"
	pb "my_rpc"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NameNodeServer implements the NameNode service
type NameNodeServer struct {
	pb.UnimplementedNameNodeServiceServer // Required for forward compatibility with gRPC

	// Internal state
	metadata  map[string]*pb.FileMetadata     // path -> metadata
	datanodes map[string]*pb.DatanodeLocation // datanode_id -> location
	mutex     sync.RWMutex

	// Add file locking system
	fileLocks  *sync.RWMutex          // path -> lock
	lockHandle map[string]*LockHandle // path -> list of lock holders

	// Add heartbeat tracking
	heartbeats     map[string]int64 // datanode_id -> last heartbeat timestamp
	heartbeatMutex sync.RWMutex
}

type LockHandle struct {
	expiryAtN int64
	writer    int8
	readers   int32
}

// NewNameNodeServer creates a new NameNode server instance
func NewNameNodeServer() *NameNodeServer {
	namnode_server := &NameNodeServer{
		metadata:   make(map[string]*pb.FileMetadata),
		datanodes:  make(map[string]*pb.DatanodeLocation),
		fileLocks:  &sync.RWMutex{},
		lockHandle: make(map[string]*LockHandle),
		heartbeats: make(map[string]int64),
	}
	namnode_server.MakeDirectory(context.Background(), &pb.ListDirectoryRequest{Path: "/", Permission: 0777, Owner: "root"})

	// Start heartbeat checker
	go namnode_server.checkHeartbeats()

	return namnode_server
}

// Helper function to check permissions
func (s *NameNodeServer) checkPermissions(path string, owner string, requiredPermission uint32) error {
	metadata, exists := s.metadata[path]
	if !exists {
		return errors.New("file not found")
	}
	if metadata.GetOwner() != owner && owner != "root" {
		return errors.New("not the owner")
	}
	if metadata.Permission&requiredPermission == 0 {
		return errors.New("insufficient permissions")
	}
	return nil
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
		Size:             req.GetSize(),
		Permission:       req.GetPermission(),
		Owner:            req.GetOwner(), // Current user
		CreationTime:     time.Now().UnixNano(),
		ModificationTime: time.Now().UnixNano(),
		Version:          0,
		Replicas:         selectedNodes,
	}

	// Update metadata
	s.metadata[path] = metadata

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
	metadata, exists := s.metadata[path]
	if !exists {
		return &pb.DeleteFileResponse{
			Success:      false,
			ErrorMessage: "file not found",
		}, nil
	}
	// Delete file from all DataNodes first
	for _, replica := range metadata.Replicas {
		// Connect to each DataNode
		datanodeAddr := fmt.Sprintf("%s:%d", replica.Host, replica.Port)
		conn, err := grpc.NewClient(datanodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Warning: Failed to connect to DataNode %s: %v\n", datanodeAddr, err)
			continue
		}
		defer conn.Close()

		// Create DataNode client and delete file
		datanodeClient := pb.NewDataNodeServiceClient(conn)
		_, err = datanodeClient.DeleteFile(ctx, &pb.DeleteFileRequest{
			Path: path,
		})
		if err != nil {
			fmt.Printf("Warning: Failed to delete file on DataNode %s: %v\n", datanodeAddr, err)
			// Continue with other replicas even if one fails
		}
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

	s.fileLocks.Lock()
	defer s.fileLocks.Unlock()

	lock, exists := s.lockHandle[path]

	if !exists {
		// create a lock
		s.lockHandle[path] = &LockHandle{
			expiryAtN: time.Now().Add(15 * time.Second).UnixNano(),
			writer:    0,
			readers:   0,
		}
		lock = s.lockHandle[path]
	}

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
	// 15 seconds
	const TIMEOUT = 15 * time.Second
	lock.expiryAtN = time.Now().Add(TIMEOUT).UnixNano()

	return &pb.LockFileResponse{Success: true}, nil
}

func (s *NameNodeServer) UnlockFile(ctx context.Context, req *pb.UnlockFileRequest) (*pb.UnlockFileResponse, error) {
	path := filepath.Clean(req.Path)

	s.fileLocks.Lock()
	defer s.fileLocks.Unlock()

	if lock, exists := s.lockHandle[path]; exists {
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
		// If metadata is not found, return success
		if s.metadata[path] == nil {
			return &pb.UnlockFileResponse{Success: true}, nil
		}
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

	// Initialize heartbeat tracking
	s.heartbeatMutex.Lock()
	s.heartbeats[location.DatanodeId] = time.Now().UnixNano()
	s.heartbeatMutex.Unlock()

	fmt.Printf("Registered DataNode: %s at %s:%d\n", location.DatanodeId, location.Host, location.Port)

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

// SendHeartbeat handles heartbeat messages from DataNodes
func (s *NameNodeServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.heartbeatMutex.Lock()
	s.heartbeats[req.DatanodeId] = req.Timestamp
	s.heartbeatMutex.Unlock()

	return &pb.HeartbeatResponse{Success: true}, nil
}

// checkHeartbeats periodically checks for dead DataNodes
func (s *NameNodeServer) checkHeartbeats() {
	const heartbeatTimeout = 10 * time.Second
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		now := time.Now().UnixNano()
		deadNodes := []string{}

		s.heartbeatMutex.RLock()
		for nodeID, lastBeat := range s.heartbeats {
			if now-lastBeat > int64(heartbeatTimeout) {
				deadNodes = append(deadNodes, nodeID)
			}
		}
		s.heartbeatMutex.RUnlock()

		if len(deadNodes) > 0 {
			s.mutex.Lock()
			for _, nodeID := range deadNodes {
				delete(s.datanodes, nodeID)
				s.heartbeatMutex.Lock()
				delete(s.heartbeats, nodeID)
				s.heartbeatMutex.Unlock()
				fmt.Printf("DataNode %s marked as dead\n", nodeID)
			}
			s.mutex.Unlock()
		}
	}
}

// Helper function to generate a unique datanode ID
func generateDatanodeID(host string, port int32) string {
	return fmt.Sprintf("%s:%d", host, port)
}

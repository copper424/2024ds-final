package main

import (
	"context"
	"fmt"
	pb "my_rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"syscall"
)

// DataNodeServer implements the DataNode service
type DataNodeServer struct {
	pb.UnimplementedDataNodeServiceServer // Required for forward compatibility with gRPC

	// Configuration
	dataDir string // Directory to store files
	logDir  string // Directory to store transaction logs
	logFile string // Path to the transaction log file

	// Internal state
	nodeID string // Unique identifier for this datanode
	host   string // Host address
	port   int32  // Port number

	// Add transaction tracking
	txnLock     sync.RWMutex
	pendingTxns map[string]*TxnState

	namenode string
}

// TxnState tracks the state of a transaction
type TxnState struct {
	Path    string
	Content []byte
}

// NewDataNodeServer creates a new DataNode server instance
func NewDataNodeServer(dataDir, host string, port int32, namenode string) (*DataNodeServer, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// Create log directory
	logDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %v", err)
	}

	// Set log file path
	logFile := filepath.Join(logDir, "transactions.log")

	return &DataNodeServer{
		dataDir:     dataDir,
		logDir:      logDir,
		logFile:     logFile,
		host:        host,
		port:        port,
		nodeID:      fmt.Sprintf("%s:%d", host, port),
		pendingTxns: make(map[string]*TxnState),
		namenode:    namenode,
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

	// Get replica locations from NameNode
	namenodeConn, err := grpc.NewClient(s.namenode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.WriteFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to connect to NameNode: %v", err),
		}, nil
	}
	namenodeClient := pb.NewNameNodeServiceClient(namenodeConn)
	MetadataResp, err := namenodeClient.GetFileLocations(ctx, &pb.GetFileMetadataRequest{
		Path: req.Path,
	})
	if err != nil {
		return &pb.WriteFileResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to get replica locations: %v", err),
		}, nil
	}

	// Generate transaction ID
	txnID := uuid.New().String()

	// Phase 1: Prepare
	for _, replica := range MetadataResp.GetMetadata().Replicas {
		conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", replica.Host, replica.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &pb.WriteFileResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to connect to replica: %v", err),
			}, nil
		}
		defer conn.Close()

		client := pb.NewDataNodeServiceClient(conn)
		prepareResp, err := client.PrepareReplica(ctx, &pb.PrepareReplicaRequest{
			TxnId:   txnID,
			Path:    req.Path,
			Content: req.Content,
		})

		if err != nil || !prepareResp.Success {
			// Abort on all prepared replicas
			s.AbortReplica(ctx, &pb.AbortReplicaRequest{
				TxnId: txnID,
				Path:  req.Path,
			})
			return &pb.WriteFileResponse{
				Success:      false,
				ErrorMessage: "prepare phase failed",
			}, nil
		}
	}

	// Phase 2: Commit
	for _, replica := range MetadataResp.GetMetadata().Replicas {
		conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", replica.Host, replica.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// Log error but continue since prepare was successful
			fmt.Printf("failed to connect to replica: %v\n", err)
			continue
		}
		defer conn.Close()

		client := pb.NewDataNodeServiceClient(conn)
		resp, err := client.CommitReplica(ctx, &pb.CommitReplicaRequest{
			TxnId: txnID,
			Path:  req.Path,
		})
		if err != nil {
			// Log error but continue
			fmt.Printf("failed to commit replica: %v\n", err)
		} else if !resp.Success {
			fmt.Printf("failed to commit replica: %v\n", resp.ErrorMessage)
		}
	}

	return &pb.WriteFileResponse{Success: true}, nil
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
			Size: uint64(len(content)),
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
func (s *DataNodeServer) StartDataNode(namenodeAddr string, heartbeat_interval time.Duration) error {
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

	// Start heartbeat goroutine
	go s.startHeartbeat(namenodeAddr, heartbeat_interval)

	return nil
}

// startHeartbeat periodically sends heartbeat to NameNode
func (s *DataNodeServer) startHeartbeat(namenodeAddr string, heartbeat_interval time.Duration) {
	ticker := time.NewTicker(heartbeat_interval)
	for range ticker.C {
		conn, err := grpc.NewClient(namenodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Failed to connect to NameNode for heartbeat: %v\n", err)
			continue
		}

		client := pb.NewNameNodeServiceClient(conn)
		_, err = client.SendHeartbeat(context.Background(), &pb.HeartbeatRequest{
			DatanodeId: s.nodeID,
			Timestamp:  time.Now().UnixNano(),
		})

		conn.Close()
		if err != nil {
			fmt.Printf("Failed to send heartbeat: %v\n", err)
		}
	}
}

// PrepareReplica handles the prepare phase of 2PC
func (s *DataNodeServer) PrepareReplica(ctx context.Context, req *pb.PrepareReplicaRequest) (*pb.PrepareReplicaResponse, error) {
	s.txnLock.Lock()
	defer s.txnLock.Unlock()

	// Check if we can write to the target path
	finalPath := s.getFilePath(req.Path)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return &pb.PrepareReplicaResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("cannot create directory: %v", err),
		}, nil
	}

	// Store transaction state
	s.pendingTxns[req.TxnId] = &TxnState{
		Path:    req.Path,
		Content: req.Content,
	}

	return &pb.PrepareReplicaResponse{Success: true}, nil
}

// writeTransactionLog writes transaction log by appending to the log file
func (s *DataNodeServer) writeTransactionLog(txnId string, oldContent []byte, newContent []byte) error {
	// Open file in append mode with write-only access
	f, err := os.OpenFile(s.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	// Get exclusive lock on the file
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("failed to lock log file: %v", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	// Prepare log entry
	logEntry := []byte(fmt.Sprintf("%s\n\n", txnId))
	logEntry = append(logEntry, oldContent...)
	logEntry = append(logEntry, []byte("\n\n")...)
	logEntry = append(logEntry, newContent...)
	logEntry = append(logEntry, []byte("\n\n------------------------\n\n")...) // Separator between entries

	// Append log entry
	if _, err := f.Write(logEntry); err != nil {
		return fmt.Errorf("failed to write to log file: %v", err)
	}

	// Ensure log is written to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %v", err)
	}

	return nil
}

// CommitReplica handles the commit phase of 2PC with Write-Ahead Logging
func (s *DataNodeServer) CommitReplica(ctx context.Context, req *pb.CommitReplicaRequest) (*pb.CommitReplicaResponse, error) {
	s.txnLock.Lock()
	defer s.txnLock.Unlock()

	txn, exists := s.pendingTxns[req.TxnId]
	if !exists {
		return &pb.CommitReplicaResponse{
			Success:      false,
			ErrorMessage: "transaction not found",
		}, nil
	}

	finalPath := s.getFilePath(txn.Path)

	// Read old content if file exists
	var oldContent []byte
	if _, err := os.Stat(finalPath); err == nil {
		oldContent, err = os.ReadFile(finalPath)
		if err != nil {
			return &pb.CommitReplicaResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("failed to read existing file: %v", err),
			}, nil
		}
	}

	// Write transaction log first (Write-Ahead Logging)
	if err := s.writeTransactionLog(req.TxnId, oldContent, txn.Content); err != nil {
		return &pb.CommitReplicaResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to write transaction log: %v", err),
		}, nil
	}

	// Write the actual file
	if err := os.WriteFile(finalPath, txn.Content, 0644); err != nil {
		return &pb.CommitReplicaResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to write file: %v", err),
		}, nil
	}

	// Clean up transaction state and log
	delete(s.pendingTxns, req.TxnId)
	// We can optionally remove the log file here since the transaction completed successfully
	// logPath := filepath.Join(s.logDir, fmt.Sprintf("%s.log", req.TxnId))
	// os.Remove(logPath) // Ignore error as it's not critical

	return &pb.CommitReplicaResponse{Success: true}, nil
}

// AbortReplica handles the abort phase of 2PC
func (s *DataNodeServer) AbortReplica(ctx context.Context, req *pb.AbortReplicaRequest) (*pb.AbortReplicaResponse, error) {
	s.txnLock.Lock()
	defer s.txnLock.Unlock()

	// Just clean up the transaction state
	delete(s.pendingTxns, req.TxnId)
	return &pb.AbortReplicaResponse{Success: true}, nil
}

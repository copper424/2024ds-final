package main

import (
	"context"
	"fmt"
	pb "my_rpc"
	"os"

	// "time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Modify uploadFile method to include permission parameter
func (sh *DFSShell) uploadFile(args []string) {
	if len(args) < 3 || len(args) > 4 {
		fmt.Println("Usage: put <local_file> <dfs_path> [permission]")
		fmt.Println("Permissions: private(0), readonly(1), readwrite(2)")
		return
	}

	localPath := args[1]
	dfsPath := sh.resolvePath(args[2])

	// Default permission is private
	permission := uint32(0)
	if len(args) == 4 {
		switch args[3] {
		case "0", "private":
			permission = 0
		case "1", "readonly":
			permission = 1
		case "2", "readwrite":
			permission = 2
		default:
			fmt.Println("Invalid permission. Using default (private)")
		}
	}

	content, err := os.ReadFile(localPath)
	if err != nil {
		fmt.Printf("Error reading local file: %v\n", err)
		return
	}

	ctx := context.Background()

	// First check if file exists
	metaResp, err := sh.client.GetFileMetadata(ctx, &pb.GetFileMetadataRequest{
		Path: dfsPath,
	})
	if err != nil {
		fmt.Printf("Failed to get file metadata: %v\n", err)
		return
	}

	var newMetadata *pb.FileMetadata
	if !metaResp.Success {
		// File doesn't exist, create new file
		createResp, err := sh.client.CreateFile(ctx, &pb.CreateFileRequest{
			Path:              dfsPath,
			ReplicationFactor: 3,
			Permission:        permission,
			Owner:             sh.owner,
			Size:              uint64(len(content)),
		})
		if err != nil || !createResp.Success {
			fmt.Printf("Failed to create file: %v\n", err)
			return
		}
		newMetadata = createResp.Metadata
	} else {
		newMetadata = metaResp.Metadata
	}

	// Now lock the file
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_WRITE,
		Owner:    sh.owner,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v %v\n", err, lockResp.ErrorMessage)
		return
	}
	defer sh.unlockFile(ctx, dfsPath, pb.LockType_WRITE)

	// Store the file to primar DataNode replica
	primary_datanode := newMetadata.Replicas[0]
	datanodeAddr := fmt.Sprintf("%s:%d", primary_datanode.Host, primary_datanode.Port)
	conn, err := grpc.NewClient(datanodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Failed to connect to DataNode %s: %v\n", datanodeAddr, err)
		return
	}
	defer conn.Close()

	datanodeClient := pb.NewDataNodeServiceClient(conn)
	resp, err := datanodeClient.StoreFile(ctx, &pb.WriteFileRequest{
		Path:    dfsPath,
		Content: content,
	})
	if err != nil {
		fmt.Printf("Failed to store file on DataNode %s: %v\n", datanodeAddr, err)
		return
	}
	if !resp.Success {
		fmt.Printf("Failed to store file on DataNode %s: %s\n", datanodeAddr, resp.ErrorMessage)
		return
	}

	// After successful upload, update cache
	sh.cacheMutex.Lock()
	sh.cache[dfsPath] = &FileCache{
		Content:             content,
		Metadata:            newMetadata,
		LastModifiedVersion: newMetadata.Version,
	}
	sh.cacheMutex.Unlock()

	fmt.Printf("Successfully uploaded %s to %s\n", localPath, dfsPath)
}

func (sh *DFSShell) downloadFile(args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: get <dfs_path> <local_file>")
		return
	}

	dfsPath := sh.resolvePath(args[1])
	localPath := args[2]

	ctx := context.Background()

	// First check metadata and permissions
	metaResp, err := sh.client.GetFileMetadata(ctx, &pb.GetFileMetadataRequest{
		Path: dfsPath,
	})
	if err != nil || !metaResp.Success {
		fmt.Printf("Failed to get file metadata: %v\n", err)
		return
	}

	// After confirming file exists, try to lock
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_READ,
		Owner:    sh.owner,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v %v\n", err, lockResp.ErrorMessage)
		return
	}
	defer sh.unlockFile(ctx, dfsPath, pb.LockType_READ)

	// Check cache
	sh.cacheMutex.RLock()
	cachedFile, exists := sh.cache[dfsPath]
	sh.cacheMutex.RUnlock()

	var content []byte
	if exists && int64(cachedFile.LastModifiedVersion) == int64(metaResp.Metadata.GetVersion()) {
		// Cache hit and not modified
		content = cachedFile.Content
		fmt.Println("Using cached version of file")
	} else {
		// Cache miss or file modified - fetch from DataNode
		content, err = sh.fetchFromDataNode(ctx, dfsPath, metaResp.Metadata)
		if err != nil {
			fmt.Printf("Failed to fetch file: %v\n", err)
			return
		}

		// Update cache
		sh.cacheMutex.Lock()
		sh.cache[dfsPath] = &FileCache{
			Content:             content,
			Metadata:            metaResp.Metadata,
			LastModifiedVersion: metaResp.Metadata.GetVersion(),
		}
		sh.cacheMutex.Unlock()
	}

	// Write to local file
	err = os.WriteFile(localPath, content, 0644)
	if err != nil {
		fmt.Printf("Error writing local file: %v\n", err)
		return
	}

	fmt.Printf("Successfully downloaded %s to %s\n", dfsPath, localPath)
}

// Add helper function for unlock operations
func (sh *DFSShell) unlockFile(ctx context.Context, path string, lockType pb.LockType) {
	_, err := sh.client.UnlockFile(ctx, &pb.UnlockFileRequest{
		Path:     path,
		LockType: lockType,
	})
	if err != nil {
		fmt.Printf("Failed to unlock file: %v\n", err)
	}
}

// Helper function to fetch file from DataNode
func (sh *DFSShell) fetchFromDataNode(ctx context.Context, dfsPath string, metadata *pb.FileMetadata) ([]byte, error) {
	for _, datanode := range metadata.Replicas {
		datanodeAddr := fmt.Sprintf("%s:%d", datanode.Host, datanode.Port)
		conn, err := grpc.NewClient(datanodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()

		datanodeClient := pb.NewDataNodeServiceClient(conn)
		retrieveResp, err := datanodeClient.RetrieveFile(ctx, &pb.ReadFileRequest{
			Path: dfsPath,
		})
		if err != nil || !retrieveResp.Success {
			continue
		}
		return retrieveResp.Content, nil
	}
	return nil, fmt.Errorf("failed to retrieve file from any DataNode")
}

func (sh *DFSShell) deleteFile(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: rm <dfs_path>")
		return
	}

	dfsPath := sh.resolvePath(args[1])

	ctx := context.Background()

	// Step 1: LockFile on NameNode
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_WRITE,
		Owner:    sh.owner,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v %v\n", err, lockResp.ErrorMessage)
		return
	}
	defer func() {
		// Unlock the file on the NameNode
		_, err := sh.client.UnlockFile(ctx, &pb.UnlockFileRequest{
			Path:     dfsPath,
			LockType: pb.LockType_WRITE,
		})
		if err != nil {
			fmt.Printf("Failed to unlock file: %v\n", err)
		}
	}()

	// Step 2: GetFileMetadata to get DataNode locations
	metaResp, err := sh.client.GetFileMetadata(ctx, &pb.GetFileMetadataRequest{
		Path: dfsPath,
	})
	if err != nil || !metaResp.Success {
		fmt.Printf("Failed to get file metadata: %v\n", err)
		return
	}

	// Step 3: DeleteFile on NameNode. The Namenode will take care of deleting the file from all DataNodes
	deleteResp, err := sh.client.DeleteFile(ctx, &pb.DeleteFileRequest{
		Path: dfsPath,
	})
	if err != nil || !deleteResp.Success {
		fmt.Printf("Failed to delete file on NameNode: %v\n", err)
		return
	}

	fmt.Printf("Successfully deleted %s\n", dfsPath)
}

func (sh *DFSShell) catFile(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: cat <dfs_path>")
		return
	}

	dfsPath := sh.resolvePath(args[1])

	ctx := context.Background()

	// Step 1: LockFile on NameNode
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_READ,
		Owner:    sh.owner,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v %v\n", err, lockResp.ErrorMessage)
		return
	}
	defer func() {
		// UnlockFile on NameNode
		_, err := sh.client.UnlockFile(ctx, &pb.UnlockFileRequest{
			Path:     dfsPath,
			LockType: pb.LockType_READ,
		})
		if err != nil {
			fmt.Printf("Failed to unlock file: %v\n", err)
		}
	}()

	// Step 2: GetFileMetadata to get DataNode locations
	metaResp, err := sh.client.GetFileMetadata(ctx, &pb.GetFileMetadataRequest{
		Path: dfsPath,
	})
	if err != nil || !metaResp.Success {
		fmt.Printf("Failed to get file metadata: %v\n", err)
		return
	}

	// Step 3: RetrieveFile from one of the DataNodes
	var content []byte
	for _, datanode := range metaResp.Metadata.Replicas {
		datanodeAddr := fmt.Sprintf("%s:%d", datanode.Host, datanode.Port)
		conn, err := grpc.NewClient(datanodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Failed to connect to DataNode %s: %v\n", datanodeAddr, err)
			continue
		}
		defer conn.Close()

		datanodeClient := pb.NewDataNodeServiceClient(conn)
		retrieveResp, err := datanodeClient.RetrieveFile(ctx, &pb.ReadFileRequest{
			Path: dfsPath,
		})
		if err != nil || !retrieveResp.Success {
			fmt.Printf("Failed to retrieve file from DataNode %s: %v\n", datanodeAddr, err)
			continue
		}
		content = retrieveResp.Content
		break
	}

	if content == nil {
		fmt.Println("Failed to retrieve file from any DataNode.")
		return
	}

	fmt.Print(string(content))
}

func (sh *DFSShell) moveFile(args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: mv <src> <dest>")
		return
	}

	srcPath := sh.resolvePath(args[1])
	destPath := sh.resolvePath(args[2])

	resp, err := sh.client.MoveFile(context.Background(), &pb.ReadFileRequest{})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to move file: %s\n", resp.ErrorMessage)
		return
	}

	fmt.Printf("File moved from %s to %s successfully\n", srcPath, destPath)
}

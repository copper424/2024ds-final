package main

import (
	"bufio"
	"context"
	"fmt"
	pb "my_rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DFSShell struct {
	client     pb.NameNodeServiceClient
	currentDir string
}

func NewDFSShell(serverAddr string) (*DFSShell, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &DFSShell{
		client:     pb.NewNameNodeServiceClient(conn),
		currentDir: "/",
	}, nil
}

func (sh *DFSShell) Run() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to DFS Shell. Type 'help' for commands.")

	for {
		fmt.Printf("dfs:%s> ", sh.currentDir)
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		args := strings.Fields(input)
		command := args[0]

		switch command {
		case "help":
			sh.printHelp()
		case "ls":
			sh.listFiles(args)
		case "cd":
			sh.changeDirectory(args)
		case "put":
			sh.uploadFile(args)
		case "get":
			sh.downloadFile(args)
		// case "rm":
		// 	sh.deleteFile(args)
		// case "mkdir":
		// 	sh.makeDirectory(args)
		case "pwd":
			fmt.Println(sh.currentDir)
		// case "cat":
		// 	sh.catFile(args)
		case "stat":
			sh.showFileMetadata(args)
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Printf("Unknown command: %s\nType 'help' for available commands\n", command)
		}
	}
}

func (sh *DFSShell) printHelp() {
	fmt.Println(`
Available commands:
    ls [path]           - List files in directory
    cd <path>           - Change current directory
    pwd                 - Print current directory
    put <local> <dfs>   - Upload local file to DFS
    get <dfs> <local>   - Download file from DFS
    rm <path>           - Delete file or directory
    mkdir <path>        - Create directory
    cat <path>          - Display file contents
    stat <path>         - Show file metadata
    help                - Show this help
    exit/quit           - Exit shell`)
}

func (sh *DFSShell) listFiles(args []string) {
	path := sh.currentDir
	if len(args) > 1 {
		path = sh.resolvePath(args[1])
	}

	resp, err := sh.client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path: path,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to list directory: %s\n", resp.ErrorMessage)
		return
	}

	fmt.Printf("Contents of %s:\n", path)
	for _, file := range resp.Files {
		fmt.Printf("%s\t%d bytes\t%s\n", file.Filename, file.Size,
			formatTime(file.ModificationTime))
	}
}

func (sh *DFSShell) uploadFile(args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: put <local_file> <dfs_path>")
		return
	}

	localPath := args[1]
	dfsPath := sh.resolvePath(args[2])

	content, err := os.ReadFile(localPath)
	if err != nil {
		fmt.Printf("Error reading local file: %v\n", err)
		return
	}

	ctx := context.Background()

	// Step 1: CreateFile on NameNode
	createResp, err := sh.client.CreateFile(ctx, &pb.CreateFileRequest{
		Path: dfsPath,
	})
	if err != nil || !createResp.Success {
		fmt.Printf("Failed to create file: %v\n", err)
		return
	}

	// Step 2: LockFile on NameNode
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_WRITE,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v\n", err)
		return
	}
	defer func() {
		// UnlockFile on NameNode
		_, err := sh.client.UnlockFile(ctx, &pb.UnlockFileRequest{
			Path:     dfsPath,
			LockType: pb.LockType_WRITE,
		})
		if err != nil {
			fmt.Printf("Failed to unlock file: %v\n", err)
		}
	}()

	// Step 3: GetFileMetadata to get DataNode locations
	metaResp, err := sh.client.GetFileMetadata(ctx, &pb.GetFileMetadataRequest{
		Path: dfsPath,
	})
	if err != nil || !metaResp.Success {
		fmt.Printf("Failed to get file metadata: %v\n", err)
		return
	}

	// Step 4: StoreFile on each DataNode
	for _, datanode := range metaResp.Metadata.Replicas {
		datanodeAddr := fmt.Sprintf("%s:%d", datanode.Host, datanode.Port)
		conn, err := grpc.NewClient(datanodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Failed to connect to DataNode %s: %v\n", datanodeAddr, err)
			continue
		}
		defer conn.Close()

		datanodeClient := pb.NewDataNodeServiceClient(conn)
		_, err = datanodeClient.StoreFile(ctx, &pb.WriteFileRequest{
			Path:    dfsPath,
			Content: content,
		})
		if err != nil {
			fmt.Printf("Failed to store file on DataNode %s: %v\n", datanodeAddr, err)
			continue
		}
	}

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

	// Step 1: LockFile on NameNode
	lockResp, err := sh.client.LockFile(ctx, &pb.LockFileRequest{
		Path:     dfsPath,
		LockType: pb.LockType_READ,
	})
	if err != nil || !lockResp.Success {
		fmt.Printf("Failed to lock file: %v\n", err)
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

	err = os.WriteFile(localPath, content, 0644)
	if err != nil {
		fmt.Printf("Error writing local file: %v\n", err)
		return
	}

	fmt.Printf("Successfully downloaded %s to %s\n", dfsPath, localPath)
}

// func (sh *DFSShell) catFile(args []string) {
// 	if len(args) != 2 {
// 		fmt.Println("Usage: cat <dfs_path>")
// 		return
// 	}

// 	path := sh.resolvePath(args[1])
// 	resp, err := sh.client.ReadFile(context.Background(), &pb.ReadFileRequest{
// 		Path: path,
// 	})
// 	if err != nil {
// 		fmt.Printf("Error: %v\n", err)
// 		return
// 	}

// 	if !resp.Success {
// 		fmt.Printf("Failed to read file: %s\n", resp.ErrorMessage)
// 		return
// 	}

// 	fmt.Println(string(resp.Content))
// }

func (sh *DFSShell) changeDirectory(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: cd <path>")
		return
	}

	newPath := sh.resolvePath(args[1])

	// Verify directory exists
	resp, err := sh.client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path: newPath,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to change directory: %s\n", resp.ErrorMessage)
		return
	}

	sh.currentDir = newPath
}

func (sh *DFSShell) showFileMetadata(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: stat <path>")
		return
	}

	path := sh.resolvePath(args[1])
	resp, err := sh.client.GetFileMetadata(context.Background(), &pb.GetFileMetadataRequest{
		Path: path,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to get metadata: %s\n", resp.ErrorMessage)
		return
	}

	meta := resp.Metadata
	fmt.Printf("File: %s\n", meta.Filename)
	fmt.Printf("Size: %d bytes\n", meta.Size)
	fmt.Printf("Permission: %o\n", meta.Permission)
	fmt.Printf("Owner: %s\n", meta.Owner)
	fmt.Printf("Created: %s\n", formatTime(meta.CreationTime))
	fmt.Printf("Modified: %s\n", formatTime(meta.ModificationTime))
	fmt.Printf("Replicas: %d\n", len(meta.Replicas))
}

// Helper function to resolve relative paths
func (sh *DFSShell) resolvePath(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	return filepath.Join(sh.currentDir, path)
}

// Helper function to format timestamps
func formatTime(timestamp int64) string {
	return time.Unix(0, timestamp).Format("2006-01-02 15:04:05")
}

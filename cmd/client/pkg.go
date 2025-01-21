package main

import (
	"bufio"
	"context"
	"fmt"
	pb "my_rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FileCache represents a cached file
type FileCache struct {
	Content             []byte
	Metadata            *pb.FileMetadata
	LastModifiedVersion uint64
}

type DFSShell struct {
	client     pb.NameNodeServiceClient
	currentDir string
	cache      map[string]*FileCache // path -> cache entry
	cacheMutex sync.RWMutex
}

func NewDFSShell(serverAddr string) (*DFSShell, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &DFSShell{
		client:     pb.NewNameNodeServiceClient(conn),
		currentDir: "/",
		cache:      make(map[string]*FileCache),
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
		case "rm":
			sh.deleteFile(args)
		// case "mkdir":
		// 	sh.makeDirectory(args)
		case "pwd":
			fmt.Println(sh.currentDir)
		case "cat":
			sh.catFile(args)
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
	fmt.Printf("Version: %d\n", meta.Version)
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

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
	owner      string
	cache      map[string]*FileCache // path -> cache entry
	cacheMutex sync.RWMutex
}

func NewDFSShell(serverAddr string, owner string) (*DFSShell, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &DFSShell{
		client:     pb.NewNameNodeServiceClient(conn),
		currentDir: "/",
		owner:      owner,
		cache:      make(map[string]*FileCache),
		cacheMutex: sync.RWMutex{},
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
		case "mv":
			sh.moveFile(args)
		case "rm":
			sh.deleteFile(args)
		case "mkdir":
			sh.makeDirectory(args)
		case "rmdir":
			sh.deleteDirectory(args)
		case "pwd":
			fmt.Println(sh.currentDir)
		case "cat":
			sh.catFile(args)
		case "stat":
			sh.showFileMetadata(args)
		case "chmod":
			sh.changeMode(args)
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
DFS Shell Commands:
File Operations:
    put <local> <dfs> [perm]  - Upload local file to DFS
                                perm: private(0), readonly(1), readwrite(2)
    get <dfs> <local>         - Download file from DFS
    cat <path>                - Display file contents
    rm <path>                 - Delete file
    mv <src> <dest>          - Move/rename file
    chmod <perm> <path>       - Change file permissions
                                perm: private(0), readonly(1), readwrite(2)

Directory Operations:
    ls [path]                - List directory contents
    cd <path>                - Change current directory
    pwd                      - Print working directory
    mkdir <path>             - Create directory
    rmdir <path>             - Remove directory

Other Commands:
    stat <path>              - Show file/directory metadata
    help                     - Show this help message
    exit, quit              - Exit the shell

Permission Levels:
    0 (private)   - Only owner can read/write
    1 (readonly)  - Owner can read/write, others can read
    2 (readwrite) - Anyone can read/write`)
}

func (sh *DFSShell) listFiles(args []string) {
	path := sh.currentDir
	if len(args) > 1 {
		path = sh.resolvePath(args[1])
	}

	resp, err := sh.client.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path:       path,
		Owner:      sh.owner,
		Permission: 0755,
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
		Path:       newPath,
		Owner:      sh.owner,
		Permission: 0755,
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

func (sh *DFSShell) makeDirectory(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: mkdir <path>")
		return
	}

	path := sh.resolvePath(args[1])
	resp, err := sh.client.MakeDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path:       path,
		Owner:      sh.owner,
		Permission: 0755,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to create directory: %s\n", resp.ErrorMessage)
		return
	}

	fmt.Printf("Directory %s created successfully\n", path)
}

func (sh *DFSShell) deleteDirectory(args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: rmdir <path>")
		return
	}

	path := sh.resolvePath(args[1])
	resp, err := sh.client.RemoveDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path:       path,
		Owner:      sh.owner,
		Permission: 0755,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to delete directory: %s\n", resp.ErrorMessage)
		return
	}

	fmt.Printf("Directory %s deleted successfully\n", path)
}

func (sh *DFSShell) changeMode(args []string) {
	if len(args) != 3 {
		fmt.Println("Usage: chmod <permission> <path>")
		fmt.Println("Permissions: private(0), readonly(1), readwrite(2)")
		return
	}

	// Parse permission
	var permission uint32
	switch args[1] {
	case "0", "private":
		permission = 0
	case "1", "readonly":
		permission = 1
	case "2", "readwrite":
		permission = 2
	default:
		fmt.Println("Invalid permission. Use: private(0), readonly(1), readwrite(2)")
		return
	}

	path := sh.resolvePath(args[2])

	resp, err := sh.client.Chmod(context.Background(), &pb.ChmodRequest{
		Path:       path,
		Permission: permission,
		Owner:      sh.owner,
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if !resp.Success {
		fmt.Printf("Failed to change permission: %s\n", resp.ErrorMessage)
		return
	}

	fmt.Printf("Successfully changed permission of %s to %d\n", path, permission)
}

package main

import (
	"flag"
	"fmt"
	"log"
	pb "my_rpc"
	"net"
	"time"

	"google.golang.org/grpc"
)

func main() {
	dataDir := flag.String("dataDir", "./datadir1", "Directory to store data files")
	host := flag.String("host", "localhost", "Host address")
	port := flag.Int("port", 20251, "Port number")
	heartbeat_interval := flag.Int("heartbeat_interval", 8, "Heartbeat interval in seconds")
	nameNodeAddr := flag.String("namenode", "localhost:20241", "Address of the NameNode")

	flag.Parse()

	datanode, err := NewDataNodeServer(
		*dataDir,
		*host,
		int32(*port),
		*nameNodeAddr,
	)
	if err != nil {
		log.Fatalf("Failed to create datanode: %v", err)
	}

	// Register with NameNode
	if err := datanode.StartDataNode(*nameNodeAddr, time.Duration((*heartbeat_interval)*int(time.Second))); err != nil {
		log.Fatalf("Failed to start datanode: %v", err)
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(server, datanode)

	log.Printf("DataNode starting on port %d", *port)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

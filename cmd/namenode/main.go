package main

import (
	"context"
	"flag"
	"log"
	pb "my_rpc"
	"net"
	"time"

	"google.golang.org/grpc"
)

func main() {
	restoreFile := flag.String("restoreFile", "", "Path to local metadata backup for restore")
	backupFile := flag.String("backupFile", "", "Path to local metadata backup for periodic saving")
	heartbeat_timeout := flag.Int("heartbeat_timeout", 10, "Heartbeat timeout in seconds")
	secondary_namenode := flag.String("secondary_namenode", "", "Address of the secondary namenode")
	flag.Parse()

	namenode := NewNameNodeServer(time.Duration(*heartbeat_timeout) * time.Second)

	if *restoreFile != "" {
		namenode.RestoreMetadata(*restoreFile)
	}

	go func() {
		for {
			time.Sleep(time.Minute / 10)
			if *backupFile != "" {
				namenode.BackupMetadata(*backupFile)
			}
			namenode.SendMetadataToSecondary(context.Background(), *secondary_namenode)
		}
	}()

	lis, err := net.Listen("tcp", "localhost:20241")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterNameNodeServiceServer(server, namenode)

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

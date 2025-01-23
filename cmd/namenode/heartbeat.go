package main

import (
	"context"
	"fmt"
	pb "my_rpc"
	"time"
)

// SendHeartbeat handles heartbeat messages from DataNodes
func (s *NameNodeServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.heartbeatMutex.Lock()
	s.heartbeats[req.DatanodeId] = req.Timestamp
	s.heartbeatMutex.Unlock()

	return &pb.HeartbeatResponse{Success: true}, nil
}

// checkHeartbeats periodically checks for dead DataNodes
func (s *NameNodeServer) checkHeartbeats() {
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		now := time.Now().UnixNano()
		deadNodes := []string{}

		s.heartbeatMutex.RLock()
		for nodeID, lastBeat := range s.heartbeats {
			if now-lastBeat > int64(s.heartbeats_timeout) {
				fmt.Printf("DataNode %s missed heartbeat. Time: %s LastBeat: %s\n", nodeID,
					time.Unix(0, now).Format(time.RFC3339),
					time.Unix(0, lastBeat).Format(time.RFC3339))
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

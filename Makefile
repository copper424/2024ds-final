# Makefile

.PHONY: all namenode secondary_namenode datanode client rpc clean

BIN_DIR := bin

all: namenode secondary_namenode datanode client 

namenode:
	go build -o $(BIN_DIR)/namenode ./cmd/namenode

secondary_namenode:
	go build -o $(BIN_DIR)/secondary_namenode ./cmd/secondary_namenode

datanode:
	go build -o $(BIN_DIR)/datanode ./cmd/datanode

client:
	go build -o $(BIN_DIR)/client ./cmd/client

rpc: 
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./pkg/my_filesystem.proto

clean:
	rm -f $(BIN_DIR)/*
	rm -f pkg/*.pb.go 
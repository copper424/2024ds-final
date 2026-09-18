module main

go 1.25.0

require (
	google.golang.org/grpc v1.83.2
	my_rpc v0.0.0-00010101000000-000000000000
)

replace my_rpc => /home/scc/wy/ds-final/pkg/

require (
	github.com/google/uuid v1.6.0
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

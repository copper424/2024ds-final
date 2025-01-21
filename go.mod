module main

go 1.23.2

require (
	google.golang.org/grpc v1.68.1
	my_rpc v0.0.0-00010101000000-000000000000
)

replace my_rpc => /home/scc/wy/ds-final/pkg/

require (
	github.com/google/uuid v1.6.0
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/protobuf v1.35.2 // indirect
)

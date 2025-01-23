package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	owner := flag.String("owner", "Alice", "Owner of the DFS shell")
	flag.Parse()

	shell, err := NewDFSShell("localhost:20241", *owner)
	if err != nil {
		fmt.Printf("Failed to create shell: %v\n", err)
		os.Exit(1)
	}

	shell.Run()
}

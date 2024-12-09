package main

import (
	"fmt"
	"os"
)

func main() {
	shell, err := NewDFSShell("localhost:20241")
	if err != nil {
		fmt.Printf("Failed to create shell: %v\n", err)
		os.Exit(1)
	}

	shell.Run()
}

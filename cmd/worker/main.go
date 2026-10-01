package main

import (
	"fmt"
	"os"

	"github.com/username/example-service/initiator"
)

func main() {
	if err := initiator.InitiateWorker(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

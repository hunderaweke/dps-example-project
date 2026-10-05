package main

import (
	"fmt"
	"os"

	"github.com/hunderaweke/dps-audit-service/initiator"
)

func main() {
	if err := initiator.InitiateWorker(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hunderaweke/dps-audit-service/initiator"
)

func main() {
	openapi := flag.Bool("openapi", false, "print the OpenAPI spec as YAML and exit")
	flag.Parse()

	if *openapi {
		spec, err := initiator.OpenAPI()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_, _ = os.Stdout.Write(spec)
		return
	}

	if err := initiator.InitiateAPI(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

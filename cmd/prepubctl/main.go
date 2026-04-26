package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "usage: prepubctl <command> [args]\n")
		os.Exit(1)
	}

	cmd := flag.Args()[0]

	switch cmd {
	case "status":
		fmt.Println("status: not implemented")
	case "abort":
		fmt.Println("abort: not implemented")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

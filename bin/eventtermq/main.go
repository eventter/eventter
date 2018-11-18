package main

import (
	"fmt"
	"os"

	"eventter.io/mq/cmd"
)

func main() {
	if err := cmd.Cmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

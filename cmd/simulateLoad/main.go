package main

import (
	"os"

	"github.com/jaiswaladi246/streampulse/internal/simulator"
)

func main() {
	os.Exit(simulator.Main(os.Args[1:], os.Stdout, os.Stderr))
}

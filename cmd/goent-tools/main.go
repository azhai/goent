package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "id-compact":
		runIDCompact(os.Args[2:])
	case "pg-optimize":
		runPgOptimize(os.Args[2:])
	case "db-transit":
		runDBTransit(os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("goent-tools - Database utility tools for goent")
	fmt.Println()
	fmt.Println("Usage: goent-tools <command> [options] [arguments]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  id-compact     Compact auto-increment ID gaps and reset sequences")
	fmt.Println("  pg-optimize    Analyze PostgreSQL indexes and provide optimization recommendations")
	fmt.Println("  db-transit     Import/export table structure and data (JSON Lines format)")
	fmt.Println()
	fmt.Println("Run 'goent-tools <command> --help' for more information on a command.")
}

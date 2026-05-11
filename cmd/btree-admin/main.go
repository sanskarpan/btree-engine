// Command btree-admin is an offline administration tool for the B+Tree storage engine.
//
// Usage:
//
//	btree-admin [flags] <command> [args...]
//
// Commands:
//
//	stats             Print engine and buffer pool statistics
//	inspect page <id> Dump the contents of a single page
//	wal tail [n]      Print the last N WAL records (default 20)
//	checkpoint        Force a checkpoint and report the LSN
//	vacuum            Run a single vacuum pass and report results
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"btree-engine/internal/engine"
)

func main() {
	dataFile := flag.String("data", "", "path to data file (required)")
	walPath := flag.String("wal", "", "path to WAL directory (required)")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	if *dataFile == "" || *walPath == "" {
		fmt.Fprintln(os.Stderr, "error: -data and -wal flags are required")
		os.Exit(1)
	}

	cfg := engine.DefaultConfig(".")
	cfg.DataFile = *dataFile
	cfg.WALFile = *walPath
	cfg.SyncWAL = false // admin tool doesn't need strict durability

	eng, err := engine.OpenEngine(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close() //nolint

	switch args[0] {
	case "stats":
		cmdStats(eng)
	case "inspect":
		if len(args) < 3 || args[1] != "page" {
			fmt.Fprintln(os.Stderr, "usage: btree-admin inspect page <id>")
			os.Exit(1)
		}
		id, err := strconv.ParseUint(args[2], 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid page id %q: %v\n", args[2], err)
			os.Exit(1)
		}
		cmdInspectPage(eng, uint32(id))
	case "wal":
		if len(args) < 2 || args[1] != "tail" {
			fmt.Fprintln(os.Stderr, "usage: btree-admin wal tail [n]")
			os.Exit(1)
		}
		n := 20
		if len(args) >= 3 {
			if v, err := strconv.Atoi(args[2]); err == nil && v > 0 {
				n = v
			}
		}
		cmdWALTail(eng, n)
	case "checkpoint":
		cmdCheckpoint(eng)
	case "vacuum":
		cmdVacuum(eng)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[0])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `btree-admin: offline B+Tree storage engine tool

Usage:
  btree-admin -data <file> -wal <dir> <command> [args...]

Commands:
  stats                 Print engine and buffer pool statistics (JSON)
  inspect page <id>     Dump raw page contents as JSON
  wal tail [n]          Print last N WAL records (default 20)
  checkpoint            Force a full checkpoint
  vacuum                Run a single vacuum pass

Flags:`)
	flag.PrintDefaults()
}

func cmdStats(eng *engine.StorageEngine) {
	s := eng.Stats()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(s) //nolint
}

func cmdInspectPage(eng *engine.StorageEngine, pageID uint32) {
	result, err := eng.PageContents(pageID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect page %d: %v\n", pageID, err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(result) //nolint
}

func cmdWALTail(eng *engine.StorageEngine, n int) {
	records := eng.WALTail(n)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(records) //nolint
}

func cmdCheckpoint(eng *engine.StorageEngine) {
	lsn, err := eng.Checkpoint()
	if err != nil {
		fmt.Fprintf(os.Stderr, "checkpoint failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("checkpoint written at LSN %d\n", lsn)
}

func cmdVacuum(eng *engine.StorageEngine) {
	eng.RunVacuum()
	fmt.Println("vacuum complete")
}

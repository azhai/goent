package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/azhai/goent/reverse"
	"golang.org/x/tools/go/packages"
)

// MinFileSize a file below this size DONT has any importent content
const MinFileSize = 150

var (
	origPath string
	force    bool

	revConfig = new(reverse.ReverseConfig)
	pkgConfig = &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo,
		Tests: false,
	}
)

func main() {
	fieldsCmd := flag.NewFlagSet("fields", flag.ExitOnError)
	fieldsCmd.BoolVar(&force, "force", false, "Force overwrite existing files")
	fieldsCmd.Usage = genUsage(fieldsCmd)

	tablesCmd := flag.NewFlagSet("tables", flag.ExitOnError)
	tablesCmd.BoolVar(&force, "force", false, "Force overwrite existing files")
	tablesCmd.StringVar(&revConfig.DatabaseDSN, "dsn", os.Getenv("GOE_DATABASE_DSN"), "Database DSN")
	tablesCmd.StringVar(&revConfig.SchemaName, "schema", "public", "Schema name to reverse")
	tablesCmd.StringVar(&revConfig.TablePrefix, "prefix", "", "Table prefix filter (only reverse tables with this prefix)")
	tablesCmd.Usage = genUsage(tablesCmd)

	cmdArgs := os.Args[1:]
	last := len(cmdArgs) - 1
	if last < 0 {
		fmt.Println("expected 'fields' or 'tables' subcommands")
		genUsage(nil)()
		os.Exit(1)
	}

	switch act := strings.ToLower(cmdArgs[0]); act {
	default:
		fmt.Printf("Invalid action: %s\n", act)
		genUsage(nil)()
		os.Exit(1)
	case "fields":
		fieldsCmd.Parse(cmdArgs[1:])
		origPath = fieldsCmd.Arg(0)
		pkgPath := getPkgPath(origPath)
		outputPath := filepath.Join(origPath, "fields.go")
		if info, err := os.Stat(outputPath); !force && err == nil && info.Size() > MinFileSize {
			outputPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunFieldsGeneration(pkgConfig, pkgPath, outputPath); err != nil {
			fmt.Printf("Error generating fields: %v\n", err)
			os.Exit(1)
		}
	case "tables":
		tablesCmd.Parse(cmdArgs[1:])
		origPath = tablesCmd.Arg(0)
		if revConfig.DatabaseDSN == "" {
			fmt.Println("Error: --dsn is required for tables action")
			os.Exit(1)
		}
		outputPath := filepath.Join(origPath, "tables.go")
		if info, err := os.Stat(outputPath); !force && err == nil && info.Size() > MinFileSize {
			outputPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunTablesGeneration(revConfig, outputPath); err != nil {
			fmt.Printf("Error generating tables: %v\n", err)
			os.Exit(1)
		}
	}
}

func genUsage(cmd *flag.FlagSet) func() {
	return func() {
		fmt.Println("Usage: goent-gen <action> [flags] <package-path>")
		if cmd != nil {
			fmt.Println("\nFlags:")
			cmd.PrintDefaults()
		}
		fmt.Println("\nExamples:")
		fmt.Println("  goent-gen fields --force ./models")
		fmt.Println("  goent-gen tables --dsn 'postgres://user:pass@localhost/db?sslmode=false' ./models")
		fmt.Println("  goent-gen tables --schema public --prefix 't_' ./models")
	}
}

func getPkgPath(origPath string) string {
	pkgPath := origPath
	if _, err := os.Stat(pkgPath); err == nil {
		absPath, err := filepath.Abs(pkgPath)
		if err == nil {
			pkgConfig.Dir = absPath
			pkgPath = "."
		}
	}
	return pkgPath
}

// GenFileExtName generates the file extension name with the modTime.
func GenFileExtName(modTime time.Time) string {
	if modTime.IsZero() {
		return ".gen"
	}
	return fmt.Sprintf(".gen-%s", modTime.Format("20060102"))
}

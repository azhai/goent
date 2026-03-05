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

	projectCmd := flag.NewFlagSet("project", flag.ExitOnError)
	projectCmd.BoolVar(&force, "force", false, "Force overwrite existing files")
	projectCmd.Usage = genUsage(projectCmd)

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
		outPath := filepath.Join(origPath, "fields.go")
		if info, err := os.Stat(outPath); !force && err == nil && info.Size() > MinFileSize {
			outPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunFieldsGeneration(pkgConfig, pkgPath, outPath); err != nil {
			fmt.Printf("Error generating fields: %v\n", err)
			os.Exit(1)
		}
	case "project":
		projectCmd.Parse(cmdArgs[1:])
		origPath = projectCmd.Arg(0)
		revConfig.FixConfigData()
		pkgPath := getPkgPath(origPath)
		pkgName, err := getPkgName(pkgConfig, pkgPath)
		if pkgName == "" {
			fmt.Printf("Error loading package %s: %v\n", pkgPath, err)
			os.Exit(1)
		}
		if revConfig.DatabaseDSN == "" {
			fmt.Println("Error: --dsn is required for project action")
			os.Exit(1)
		}
		outPath := filepath.Join(origPath, "conn.go")
		if info, err := os.Stat(outPath); !force && err == nil && info.Size() > MinFileSize {
			outPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunProjectGeneration(revConfig, pkgName, outPath); err != nil {
			fmt.Printf("Error generating project: %v\n", err)
			os.Exit(1)
		}
	case "tables":
		tablesCmd.Parse(cmdArgs[1:])
		origPath = tablesCmd.Arg(0)
		revConfig.FixConfigData()
		// fmt.Printf("ReverseConfig: %+v\n", revConfig)
		pkgPath := getPkgPath(origPath)
		pkgName, err := getPkgName(pkgConfig, pkgPath)
		if pkgName == "" {
			fmt.Printf("Error loading package %s: %v\n", pkgPath, err)
			os.Exit(1)
		}
		if revConfig.DatabaseDSN == "" {
			fmt.Println("Error: --dsn is required for tables action")
			os.Exit(1)
		}
		outPath := filepath.Join(origPath, "tables.go")
		if info, err := os.Stat(outPath); !force && err == nil && info.Size() > MinFileSize {
			outPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunTablesGeneration(revConfig, pkgName, outPath); err != nil {
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

func getPkgName(cfg *packages.Config, pkgPath string) (string, error) {
	pkgs, err := packages.Load(cfg, pkgPath)
	if err == nil && len(pkgs) > 0 {
		return pkgs[0].Name, nil
	}
	return "", err
}

// GenFileExtName generates the file extension name with the modTime.
func GenFileExtName(modTime time.Time) string {
	if modTime.IsZero() {
		return ".gen"
	}
	return fmt.Sprintf(".gen-%s", modTime.Format("20060102"))
}

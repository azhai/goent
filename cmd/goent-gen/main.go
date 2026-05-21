package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/azhai/goent/reverse"
	"golang.org/x/tools/go/packages"
)

const MinFileSize = 150

var pkgConfig = &packages.Config{
	Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
		packages.NeedTypes | packages.NeedTypesInfo,
	Tests: false,
}

func main() {
	Run()
}

func runFields(args *GenFieldsArgs) {
	origPath := args.PkgPath
	pkgPath := getPkgPath(origPath)
	outPath := filepath.Join(origPath, "fields.go")
	if info, err := os.Stat(outPath); !args.Force && err == nil && info.Size() > MinFileSize {
		outPath += GenFileExtName(info.ModTime())
	}
	if err := RunFieldsGeneration(pkgConfig, pkgPath, outPath); err != nil {
		fmt.Printf("Error generating fields: %v\n", err)
		os.Exit(1)
	}
}

func runTables(args *GenTablesArgs) {
	if args.DSN == "" {
		fmt.Fprintln(os.Stderr, "Error: --dsn is required for tables action")
		os.Exit(1)
	}
	origPath := args.PkgPath
	revConfig := &reverse.ReverseConfig{
		DatabaseDSN: args.DSN,
		SchemaName:  args.Schema,
		TablePrefix: args.Prefix,
	}
	revConfig.FixConfigData()
	pkgPath := getPkgPath(origPath)
	pkgName, err := getPkgName(pkgConfig, pkgPath)
	if pkgName == "" {
		fmt.Printf("Error loading package %s: %v\n", pkgPath, err)
		os.Exit(1)
	}
	outPath := filepath.Join(origPath, "tables.go")
	if info, err := os.Stat(outPath); !args.Force && err == nil && info.Size() > MinFileSize {
		outPath += GenFileExtName(info.ModTime())
	}
	if err := RunTablesGeneration(revConfig, pkgName, outPath); err != nil {
		fmt.Printf("Error generating tables: %v\n", err)
		os.Exit(1)
	}
}

func runProject(args *GenProjectArgs) {
	if args.DSN == "" {
		fmt.Fprintln(os.Stderr, "Error: --dsn is required for project action")
		os.Exit(1)
	}
	origPath := args.PkgPath
	revConfig := &reverse.ReverseConfig{
		DatabaseDSN: args.DSN,
	}
	revConfig.FixConfigData()
	pkgPath := getPkgPath(origPath)
	pkgName, err := getPkgName(pkgConfig, pkgPath)
	if pkgName == "" {
		fmt.Printf("Error loading package %s: %v\n", pkgPath, err)
		os.Exit(1)
	}
	outPath := filepath.Join(origPath, "conn.go")
	if info, err := os.Stat(outPath); !args.Force && err == nil && info.Size() > MinFileSize {
		outPath += GenFileExtName(info.ModTime())
	}
	if err := RunProjectGeneration(revConfig, pkgName, outPath); err != nil {
		fmt.Printf("Error generating project: %v\n", err)
		os.Exit(1)
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

func GenFileExtName(modTime time.Time) string {
	if modTime.IsZero() {
		return ".gen"
	}
	return fmt.Sprintf(".gen-%s", modTime.Format("20060102"))
}

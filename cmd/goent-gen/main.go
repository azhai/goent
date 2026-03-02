package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/tools/go/packages"
)

const MinFileSize = 128

var (
	action   string
	pkgPath  string
	origPath string

	revConfig = new(ReverseConfig)
	pkgConfig = &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles |
			packages.NeedTypes | packages.NeedTypesInfo,
		Tests: false,
	}
)

func init() {
	flag.StringVar(&revConfig.DatabaseDSN, "dsn", os.Getenv("GOE_DATABASE_DSN"), "Database DSN")
	flag.StringVar(&revConfig.SchemaName, "schema", "public", "Schema name to reverse")
	flag.StringVar(&revConfig.TablePrefix, "prefix", "", "Table prefix filter (only reverse tables with this prefix)")

	flag.Usage = func() {
		fmt.Println("Usage: goent-gen <action> <package-path> [flags]")
		fmt.Println("\nFlags:")
		flag.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Println("  goent-gen fields ./models")
		fmt.Println("  goent-gen tables ./models --dsn 'postgres://user:pass@localhost/db?sslmode=false'")
		fmt.Println("  goent-gen tables ./models --prefix t_")
	}

	cmdArgs := os.Args[1:]
	n := len(cmdArgs)
	if n < 2 || cmdArgs[n-1] == "-h" || cmdArgs[n-1] == "--help" {
		flag.Usage()
		os.Exit(0)
	}
	args := reorderArgs(cmdArgs)
	flag.CommandLine.Parse(args)

	action, pkgPath = flag.Arg(0), flag.Arg(1)
	origPath = pkgPath
	if _, err := os.Stat(pkgPath); err == nil {
		absPath, err := filepath.Abs(pkgPath)
		if err == nil {
			pkgConfig.Dir = absPath
			pkgPath = "."
		}
	}
}

func reorderArgs(args []string) []string {
	var flags, positional []string
	boolFlags := map[string]bool{"h": true, "help": true}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			name := arg[2:]
			flags = append(flags, arg)
			if _, ok := boolFlags[name]; !ok && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flags = append(flags, args[i+1])
				i++
			}
		} else {
			positional = append(positional, arg)
		}
	}
	return append(flags, positional...)
}

func main() {
	action = strings.ToLower(action)
	switch action {
	default:
		fmt.Printf("Invalid action: %s\n", action)
		os.Exit(1)
	case "fields":
		outputPath := filepath.Join(origPath, "fields.go")
		if info, err := os.Stat(outputPath); err == nil && info.Size() > MinFileSize {
			outputPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunFieldsGeneration(pkgConfig, pkgPath, outputPath); err != nil {
			fmt.Printf("Error generating fields: %v\n", err)
			os.Exit(1)
		}
	case "tables":
		if revConfig.DatabaseDSN == "" {
			fmt.Println("Error: --dsn is required for tables action")
			os.Exit(1)
		}
		outputPath := filepath.Join(origPath, "tables.go")
		if info, err := os.Stat(outputPath); err == nil && info.Size() > MinFileSize {
			outputPath += GenFileExtName(info.ModTime()) // File already exists
		}
		if err := RunTablesGeneration(revConfig, outputPath); err != nil {
			fmt.Printf("Error generating tables: %v\n", err)
			os.Exit(1)
		}
	}
}

// WriteToFile writes the formatted content to a file.
func WriteToFile(buf *bytes.Buffer, outputPath string) error {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("error formatting output: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.WriteFile(outputPath, formatted, 0644); err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}

// GenFileExtName generates the file extension name with the modTime.
func GenFileExtName(modTime time.Time) string {
	if modTime.IsZero() {
		return ".gen"
	}
	return fmt.Sprintf(".gen-%s", modTime.Format("20060102-150405"))
}

// TrimShortPrefix trims the short prefix from the name if it exists.
func TrimShortPrefix(name, prefix string) string {
	if prefix == "" {
		return name
	}
	prefixLen := len(prefix)
	if prefixLen < 2 || prefixLen > 3 {
		return name
	}
	prefixPart := strings.TrimSuffix(prefix, "_")
	if len(prefixPart) != 1 {
		return name
	}
	upperPrefix := strings.ToUpper(prefixPart)
	if strings.HasPrefix(name, upperPrefix) {
		return name[1:]
	}
	return name
}

// ToCamelCase converts snake_case to CamelCase
func ToCamelCase(s string) string {
	parts := strings.Split(s, "_")
	last := len(parts) - 1
	for i, word := range parts {
		if i == last && strings.ToLower(word) == "id" {
			parts[i] = "ID"
		} else if len(word) > 0 {
			parts[i] = strings.ToUpper(word[:1]) + strings.ToLower(word[1:])
		}
	}
	return strings.Join(parts, "")
}

// ToSnakeCase converts CamelCase to snake_case
func ToSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		word := strings.ToLower(string(r))
		result = append(result, []rune(word)...)
	}
	return string(result)
}

// ToSingular converts a plural name to its singular form.
func ToSingular(name string) string {
	if strings.HasSuffix(name, "ies") && len(name) > 3 {
		return name[:len(name)-3] + "y"
	}
	if strings.HasSuffix(name, "ses") && len(name) > 3 {
		return name[:len(name)-2]
	}
	if strings.HasSuffix(name, "s") && !strings.HasSuffix(name, "ss") && len(name) > 1 {
		return name[:len(name)-1]
	}
	return name
}

// ToPlural converts a singular name to its plural form.
func ToPlural(name string) string {
	if len(name) == 0 {
		return name
	}
	if strings.HasSuffix(name, "y") && len(name) > 1 {
		last := name[len(name)-1]
		if last != 'a' && last != 'e' && last != 'i' && last != 'o' && last != 'u' {
			return name[:len(name)-1] + "ies"
		}
	}
	if strings.HasSuffix(name, "s") || strings.HasSuffix(name, "x") ||
		strings.HasSuffix(name, "z") || strings.HasSuffix(name, "ch") ||
		strings.HasSuffix(name, "sh") {
		return name + "es"
	}
	return name + "s"
}

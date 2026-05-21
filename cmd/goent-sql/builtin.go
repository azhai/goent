package main

import (
	"fmt"
	"sort"
	"strings"
)

// BuiltinCmd represents a built-in command handler
type BuiltinCmd struct {
	Name    string
	Help    string
	Handler func(repl *REPL, args string) error
}

// BuiltinCommands returns the map of built-in commands
func BuiltinCommands() map[string]*BuiltinCmd {
	return map[string]*BuiltinCmd{
		"dt": {
			Name:    "\\dt",
			Help:    "List all tables",
			Handler: cmdListTables,
		},
		"d": {
			Name:    "\\d",
			Help:    "Describe table structure: \\d tablename",
			Handler: cmdDescribeTable,
		},
		"c": {
			Name:    "\\c",
			Help:    "Connect to database: \\c DSN",
			Handler: cmdConnect,
		},
		"set": {
			Name:    "\\set",
			Help:    "Set variable: \\set name = value (scalar, JSON, or SQL)",
			Handler: cmdSetVar,
		},
		"unset": {
			Name:    "\\unset",
			Help:    "Delete variable: \\unset name",
			Handler: cmdUnsetVar,
		},
		"vars": {
			Name:    "\\vars",
			Help:    "List all variables",
			Handler: cmdListVars,
		},
		"echo": {
			Name:    "\\echo",
			Help:    "Print variable or text (dump mode for complex types): \\echo $varname",
			Handler: cmdEcho,
		},
		"for": {
			Name:    "\\for",
			Help:    "For-in loop: \\for row in $rows { SQL }",
			Handler: cmdForLoop,
		},
		"use": {
			Name:    "\\use",
			Help:    "Set current table context: \\use tablename",
			Handler: cmdUseTable,
		},
		"schema": {
			Name:    "\\schema",
			Help:    "Set or show current schema: \\schema [name]",
			Handler: cmdSchema,
		},
		"q": {
			Name:    "\\q",
			Help:    "Quit goent-sql",
			Handler: cmdQuit,
		},
		"help": {
			Name:    "\\help",
			Help:    "Show help",
			Handler: cmdHelp,
		},
	}
}

func cmdListTables(repl *REPL, args string) error {
	tables, err := repl.executor.ListTables(repl.schema)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		fmt.Println("(no tables)")
		return nil
	}
	for _, t := range tables {
		fmt.Println(t)
	}
	fmt.Printf("(%d table(s))\n", len(tables))
	return nil
}

func cmdDescribeTable(repl *REPL, args string) error {
	table := strings.TrimSpace(args)
	if table == "" {
		table = repl.currentTable
	}
	if table == "" {
		return fmt.Errorf("usage: \\d tablename")
	}
	result, err := repl.executor.DescribeTable(table, repl.schema)
	if err != nil {
		return err
	}
	PrintResult(result)
	return nil
}

func cmdConnect(repl *REPL, args string) error {
	dsn := strings.TrimSpace(args)
	if dsn == "" {
		return fmt.Errorf("usage: \\c DSN")
	}
	cfg, err := ToDBConfig(dsn, "")
	if err != nil {
		return err
	}
	if err := repl.executor.Reconnect(cfg); err != nil {
		return err
	}
	repl.cfg = cfg
	repl.dbName = repl.executor.GetDBName(dsn)
	fmt.Printf("Connected to: %s\n", repl.dbName)
	return nil
}

func cmdSetVar(repl *REPL, args string) error {
	// Parse: \set name = value
	args = strings.TrimSpace(args)
	parts := strings.SplitN(args, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("usage: \\set name = value (scalar, JSON, or SELECT ...)")
	}
	name := strings.TrimSpace(parts[0])
	rawVal := strings.TrimSpace(parts[1])

	if name == "" {
		return fmt.Errorf("variable name cannot be empty")
	}

	// Check if value is a SQL query
	upper := strings.ToUpper(rawVal)
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") {
		expanded := repl.vars.ExpandVars(rawVal)
		result, err := repl.executor.ExecSQL(expanded)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}
		repl.vars.SetQueryResult(name, result)
		fmt.Printf("Set $%s = (%d rows)\n", name, result.Count)
		return nil
	}

	// Parse as scalar or JSON
	val, err := ParseVarValue(rawVal)
	if err != nil {
		return err
	}
	repl.vars.Set(name, val)
	fmt.Printf("Set $%s = %s\n", name, val.String())
	return nil
}

func cmdUnsetVar(repl *REPL, args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return fmt.Errorf("usage: \\unset name")
	}
	delete(repl.vars.vars, name)
	fmt.Printf("Unset $%s\n", name)
	return nil
}

func cmdListVars(repl *REPL, args string) error {
	names := repl.vars.Names()
	if len(names) == 0 {
		fmt.Println("(no variables)")
		return nil
	}
	for _, name := range names {
		val := repl.vars.vars[name]
		preview := val.String()
		if len(preview) > 80 {
			preview = preview[:77] + "..."
		}
		typ := "scalar"
		switch {
		case val.IsList():
			typ = fmt.Sprintf("list[%d]", val.Len())
		case val.IsMap():
			typ = fmt.Sprintf("rows[%d]", val.Len())
		}
		fmt.Printf("  $%-15s %-10s %s\n", name, typ, preview)
	}
	return nil
}

func cmdEcho(repl *REPL, args string) error {
	args = strings.TrimSpace(args)
	// Check if it's a variable reference for dump mode
	if strings.HasPrefix(args, "$") {
		varName := args[1:] // strip $
		// Handle $name.key or $name[0].key - extract base name
		if idx := strings.IndexByte(varName, '.'); idx > 0 {
			varName = varName[:idx]
		}
		if idx := strings.IndexByte(varName, '['); idx > 0 {
			varName = varName[:idx]
		}
		if val, ok := repl.vars.Get(varName); ok {
			dumpVar(varName, val)
			return nil
		}
	}
	// Fallback: expand and print
	expanded := repl.vars.ExpandVars(args)
	fmt.Println(expanded)
	return nil
}

// dumpVar prints a detailed dump of a variable
func dumpVar(name string, val *VarValue) {
	switch {
	case val.IsScalar():
		fmt.Printf("$%s = %s\n", name, val.Scalar())
	case val.IsList():
		fmt.Printf("$%s = list[%d]\n", name, val.Len())
		for i, item := range val.list {
			fmt.Printf("  [%d] %v\n", i, item)
		}
	case val.IsMap():
		fmt.Printf("$%s = rows[%d]\n", name, val.Len())
		for i, row := range val.maps {
			fmt.Printf("  [%d] {\n", i)
			for _, k := range sortedKeys(row) {
				fmt.Printf("    %s: %v\n", k, row[k])
			}
			fmt.Printf("  }\n")
		}
	default:
		fmt.Printf("$%s = <empty>\n", name)
	}
}

func cmdForLoop(repl *REPL, args string) error {
	// Reconstruct the full line with \for prefix
	fullLine := "for " + args
	lines := []string{fullLine}
	loop, _, err := ParseForLoop(lines)
	if err != nil {
		return err
	}
	if loop == nil {
		return fmt.Errorf("usage: \\for var in $iter { SQL }")
	}
	return repl.vars.ExecuteForLoop(loop, repl.executor)
}

func cmdUseTable(repl *REPL, args string) error {
	table := strings.TrimSpace(args)
	if table == "" {
		if repl.currentTable != "" {
			fmt.Printf("Current table: %s\n", repl.currentTable)
		} else {
			fmt.Println("(no table selected)")
		}
		return nil
	}
	repl.currentTable = table
	fmt.Printf("Using table: %s\n", table)
	return nil
}

func cmdSchema(repl *REPL, args string) error {
	schema := strings.TrimSpace(args)
	if schema == "" {
		fmt.Printf("Current schema: %s\n", repl.schema)
		return nil
	}
	repl.schema = schema
	fmt.Printf("Schema set to: %s\n", schema)
	return nil
}

func cmdQuit(repl *REPL, args string) error {
	fmt.Println("Bye!")
	repl.executor.Close()
	return fmt.Errorf("QUIT")
}

func cmdHelp(repl *REPL, args string) error {
	cmds := BuiltinCommands()
	names := make([]string, 0, len(cmds))
	for k := range cmds {
		names = append(names, k)
	}
	sort.Strings(names)

	fmt.Println("Built-in commands:")
	for _, name := range names {
		cmd := cmds[name]
		fmt.Printf("  %-12s %s\n", cmd.Name, cmd.Help)
	}
	fmt.Println()
	fmt.Println("SQL statements:")
	fmt.Println("  Enter any SQL statement ending with ; to execute")
	fmt.Println()
	fmt.Println("Variables:")
	fmt.Println("  \\set name = value          Set a variable (scalar, JSON, or SQL query)")
	fmt.Println("  \\set rows = SELECT ...     Store query result in $rows")
	fmt.Println("  $name                      Reference a variable in SQL")
	fmt.Println("  $name.key                  Access map field")
	fmt.Println("  $name[0]                   Access list index")
	fmt.Println("  $name[0].key               Combined access")
	fmt.Println()
	fmt.Println("For loops:")
	fmt.Println("  \\for row in $rows {        Iterate over query results")
	fmt.Println("    SELECT * FROM t WHERE id = $row.id")
	fmt.Println("  }")
	fmt.Println()
	fmt.Println("Echo / Dump:")
	fmt.Println("  \\echo $var                 Dump variable details (list rows, map keys)")
	fmt.Println("  \\echo text                 Print expanded text")
	return nil
}

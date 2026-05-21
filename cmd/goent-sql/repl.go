package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	prompt "github.com/c-bata/go-prompt"
)

// REPL represents the interactive read-eval-print loop
type REPL struct {
	executor     *Executor
	completer    *Completer
	vars         *VarStore
	cfg          DBConfig
	schema       string
	currentTable string
	dbName       string
	historyFile  string
}

// NewREPL creates a new REPL instance
func NewREPL(cfg DBConfig, schema string) (*REPL, error) {
	executor, err := NewExecutor(cfg)
	if err != nil {
		return nil, err
	}

	home, _ := os.UserHomeDir()
	historyFile := filepath.Join(home, ".goent-sql-history")

	repl := &REPL{
		executor:    executor,
		vars:        NewVarStore(),
		cfg:         cfg,
		schema:      schema,
		dbName:      executor.GetDBName(cfg.DSN),
		historyFile: historyFile,
	}
	repl.completer = NewCompleter(repl)
	return repl, nil
}

// Run starts the interactive REPL
func (r *REPL) Run() {
	fmt.Printf("goent-sql - Interactive SQL Query Tool\n")
	fmt.Printf("Connected to: %s (schema: %s)\n", r.dbName, r.schema)
	fmt.Printf("Type \\help for available commands\n\n")

	history := r.loadHistory()

	p := prompt.New(
		r.executeLine,
		r.completer.Complete,
		prompt.OptionTitle("goent-sql"),
		prompt.OptionPrefix(r.prompt()),
		prompt.OptionLivePrefix(func() (string, bool) {
			return r.prompt(), true
		}),
		prompt.OptionHistory(history),
		prompt.OptionSuggestionBGColor(prompt.DarkGray),
		prompt.OptionSuggestionTextColor(prompt.White),
		prompt.OptionDescriptionBGColor(prompt.DarkGray),
		prompt.OptionDescriptionTextColor(prompt.LightGray),
		prompt.OptionSelectedSuggestionBGColor(prompt.Blue),
		prompt.OptionSelectedSuggestionTextColor(prompt.White),
		prompt.OptionSelectedDescriptionBGColor(prompt.Blue),
		prompt.OptionSelectedDescriptionTextColor(prompt.White),
	)
	p.Run()
}

func (r *REPL) prompt() string {
	driver := r.cfg.Driver
	table := ""
	if r.currentTable != "" {
		table = "/" + r.currentTable
	}
	return fmt.Sprintf("goent(%s/%s%s)> ", driver, r.dbName, table)
}

// executeLine processes a single line of input
func (r *REPL) executeLine(in string) {
	in = strings.TrimSpace(in)
	if in == "" {
		return
	}

	// Save to history
	r.saveHistory(in)

	// Handle built-in commands (all start with \)
	if strings.HasPrefix(in, "\\") {
		r.handleBuiltin(in)
		return
	}

	// Handle multi-line SQL (accumulate until ;)
	if !strings.HasSuffix(in, ";") {
		// For go-prompt, we execute per line; multi-line is handled via for loop blocks
		// Single statements without ; are still executed for convenience
	}

	// Strip trailing semicolon
	sql := strings.TrimSuffix(in, ";")
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return
	}

	// Expand variables
	sql = r.vars.ExpandVars(sql)

	// Execute SQL
	result, err := r.executor.ExecSQL(sql)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	PrintResult(result)
}

// handleBuiltin processes a built-in command
func (r *REPL) handleBuiltin(in string) {
	// Parse command and args
	in = strings.TrimSpace(in)
	spaceIdx := strings.IndexByte(in, ' ')
	cmdName := in
	args := ""
	if spaceIdx > 0 {
		cmdName = in[:spaceIdx]
		args = in[spaceIdx+1:]
	}

	// Strip leading backslash
	name := strings.TrimPrefix(cmdName, "\\")

	cmds := BuiltinCommands()
	cmd, ok := cmds[name]
	if !ok {
		fmt.Printf("Unknown command: %s. Type \\help for available commands.\n", cmdName)
		return
	}

	if err := cmd.Handler(r, args); err != nil {
		if err.Error() == "QUIT" {
			os.Exit(0)
		}
		fmt.Printf("Error: %v\n", err)
	}
}

// loadHistory loads command history from file
func (r *REPL) loadHistory() []string {
	data, err := os.ReadFile(r.historyFile)
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	// Deduplicate and limit
	seen := make(map[string]bool)
	var result []string
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" && !seen[line] {
			seen[line] = true
			result = append(result, line)
		}
	}
	// Reverse to get chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	// Limit to last 500 entries
	if len(result) > 500 {
		result = result[len(result)-500:]
	}
	return result
}

// saveHistory appends a command to the history file
func (r *REPL) saveHistory(line string) {
	f, err := os.OpenFile(r.historyFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(line + "\n")
}

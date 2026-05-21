package main

import (
	"strings"

	prompt "github.com/c-bata/go-prompt"
)

// Completer provides tab-completion for the REPL
type Completer struct {
	repl *REPL
}

// NewCompleter creates a new completer
func NewCompleter(repl *REPL) *Completer {
	return &Completer{repl: repl}
}

// Complete returns completion suggestions
func (c *Completer) Complete(d prompt.Document) []prompt.Suggest {
	text := d.TextBeforeCursor()
	if text == "" {
		return nil
	}

	// Command completion (starts with \)
	if strings.HasPrefix(strings.TrimSpace(text), "\\") {
		return c.commandSuggestions(d)
	}

	// Variable completion (contains $)
	if strings.Contains(text, "$") {
		return c.variableSuggestions(d)
	}

	// SQL keyword + table/column completion
	return c.sqlSuggestions(d)
}

func (c *Completer) commandSuggestions(d prompt.Document) []prompt.Suggest {
	cmds := BuiltinCommands()
	suggestions := make([]prompt.Suggest, 0, len(cmds))
	for _, cmd := range cmds {
		suggestions = append(suggestions, prompt.Suggest{
			Text:        cmd.Name,
			Description: cmd.Help,
		})
	}
	return prompt.FilterHasPrefix(suggestions, d.GetWordBeforeCursor(), true)
}

func (c *Completer) variableSuggestions(d prompt.Document) []prompt.Suggest {
	word := d.GetWordBeforeCursor()
	if !strings.HasPrefix(word, "$") {
		return nil
	}
	prefix := word[1:] // strip $

	var suggestions []prompt.Suggest
	for _, name := range c.repl.vars.Names() {
		if strings.HasPrefix(name, prefix) {
			val := c.repl.vars.vars[name]
			typ := "scalar"
			switch {
			case val.IsList():
				typ = "list"
			case val.IsMap():
				typ = "rows"
			}
			suggestions = append(suggestions, prompt.Suggest{
				Text:        "$" + name,
				Description: typ,
			})
		}
	}
	return suggestions
}

func (c *Completer) sqlSuggestions(d prompt.Document) []prompt.Suggest {
	word := d.GetWordBeforeCursor()
	if word == "" {
		return nil
	}

	var suggestions []prompt.Suggest
	upper := strings.ToUpper(word)

	// SQL keywords
	keywords := []string{"SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "TABLE", "INDEX", "AND", "OR", "NOT", "IN", "LIKE",
		"ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "JOIN", "LEFT", "RIGHT",
		"INNER", "OUTER", "ON", "AS", "SET", "VALUES", "DISTINCT", "COUNT", "SUM",
		"AVG", "MIN", "MAX", "BETWEEN", "IS", "NULL", "EXISTS", "UNION", "ALL",
		"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "FOR", "IF", "EXISTS"}
	for _, kw := range keywords {
		if strings.HasPrefix(kw, upper) {
			suggestions = append(suggestions, prompt.Suggest{Text: kw, Description: "keyword"})
		}
	}

	// Table names
	tables, _ := c.repl.executor.ListTables(c.repl.schema)
	for _, t := range tables {
		if strings.HasPrefix(strings.ToUpper(t), upper) || strings.HasPrefix(t, word) {
			suggestions = append(suggestions, prompt.Suggest{Text: t, Description: "table"})
		}
	}

	// Current table columns
	if c.repl.currentTable != "" {
		result, _ := c.repl.executor.DescribeTable(c.repl.currentTable, c.repl.schema)
		if result != nil {
			for _, row := range result.Rows {
				colName := fmtColName(row)
				if colName != "" && (strings.HasPrefix(strings.ToUpper(colName), upper) || strings.HasPrefix(colName, word)) {
					suggestions = append(suggestions, prompt.Suggest{Text: colName, Description: "column"})
				}
			}
		}
	}

	return suggestions
}

func fmtColName(row map[string]any) string {
	// column_name for pg, name for sqlite
	for _, key := range []string{"column_name", "name"} {
		if v, ok := row[key]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

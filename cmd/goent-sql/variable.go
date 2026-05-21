package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// VarValue represents a variable value that can be a scalar, list, or map
type VarValue struct {
	scalar *string          // scalar value (string/number)
	list   []any            // JSON array
	maps   []map[string]any // query result rows or single JSON object
}

// IsScalar returns true if the value is a scalar
func (v *VarValue) IsScalar() bool { return v.scalar != nil }

// IsList returns true if the value is a list
func (v *VarValue) IsList() bool { return v.list != nil }

// IsMap returns true if the value is a map (single row or query result)
func (v *VarValue) IsMap() bool { return v.maps != nil }

// Scalar returns the scalar value as a string
func (v *VarValue) Scalar() string {
	if v.scalar != nil {
		return *v.scalar
	}
	return ""
}

// String returns a human-readable representation of the value
func (v *VarValue) String() string {
	if v.scalar != nil {
		return *v.scalar
	}
	if v.list != nil {
		b, _ := json.Marshal(v.list)
		return string(b)
	}
	if v.maps != nil {
		b, _ := json.Marshal(v.maps)
		return string(b)
	}
	return ""
}

// Len returns the length of a list or map, 1 for scalar
func (v *VarValue) Len() int {
	if v.scalar != nil {
		return 1
	}
	if v.list != nil {
		return len(v.list)
	}
	if v.maps != nil {
		return len(v.maps)
	}
	return 0
}

// VarStore manages variables in the REPL session
type VarStore struct {
	vars map[string]*VarValue
}

// NewVarStore creates a new variable store
func NewVarStore() *VarStore {
	return &VarStore{vars: make(map[string]*VarValue)}
}

// Set sets a variable value
func (s *VarStore) Set(name string, val *VarValue) {
	s.vars[name] = val
}

// SetScalar sets a scalar variable
func (s *VarStore) SetScalar(name, val string) {
	s.vars[name] = &VarValue{scalar: &val}
}

// SetQueryResult stores a query result as a variable
func (s *VarStore) SetQueryResult(name string, result *QueryResult) {
	if result == nil || len(result.Rows) == 0 {
		s.vars[name] = &VarValue{maps: []map[string]any{}}
		return
	}
	s.vars[name] = &VarValue{maps: result.Rows}
}

// Get retrieves a variable by name
func (s *VarStore) Get(name string) (*VarValue, bool) {
	v, ok := s.vars[name]
	return v, ok
}

// Names returns all variable names sorted
func (s *VarStore) Names() []string {
	names := make([]string, 0, len(s.vars))
	for k := range s.vars {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ParseVarValue parses a value string into a VarValue
// Supports: scalar, JSON object, JSON array, or SQL query
func ParseVarValue(raw string) (*VarValue, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return &VarValue{}, nil
	}

	// Try JSON array
	if strings.HasPrefix(raw, "[") {
		var list []any
		if err := json.Unmarshal([]byte(raw), &list); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
		return &VarValue{list: list}, nil
	}

	// Try JSON object
	if strings.HasPrefix(raw, "{") {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, fmt.Errorf("invalid JSON object: %w", err)
		}
		// Sort keys for consistency
		sorted := sortMapKeys(m)
		return &VarValue{maps: []map[string]any{sorted}}, nil
	}

	// Scalar value
	return &VarValue{scalar: &raw}, nil
}

// sortMapKeys returns a new map with sorted keys (for display consistency)
func sortMapKeys(m map[string]any) map[string]any {
	sorted := make(map[string]any, len(m))
	for _, k := range sortedKeys(m) {
		sorted[k] = m[k]
	}
	return sorted
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// varRefRegex matches $name, $name.key, $name[0], $name[0].key patterns
var varRefRegex = regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)(?:\[([0-9]+)\])?(?:\.([a-zA-Z_][a-zA-Z0-9_]*))?`)

// ExpandVars replaces variable references in a SQL string with their values
func (s *VarStore) ExpandVars(sql string) string {
	return varRefRegex.ReplaceAllStringFunc(sql, func(match string) string {
		parts := varRefRegex.FindStringSubmatch(match)
		name := parts[1]
		indexStr := parts[2]
		key := parts[3]

		val, ok := s.vars[name]
		if !ok {
			return match // leave unresolved
		}

		// Simple scalar: $name
		if val.IsScalar() && indexStr == "" && key == "" {
			return val.Scalar()
		}

		// List access: $name[0]
		if val.IsList() && indexStr != "" {
			idx, _ := strconv.Atoi(indexStr)
			if idx >= 0 && idx < len(val.list) {
				elem := val.list[idx]
				if key != "" {
					if m, ok := elem.(map[string]any); ok {
						if v, ok := m[key]; ok {
							return fmtVal(v)
						}
					}
					return match
				}
				return fmtVal(elem)
			}
			return match
		}

		// List as IN clause: $name without index in SQL context
		if val.IsList() && indexStr == "" && key == "" {
			items := make([]string, len(val.list))
			for i, item := range val.list {
				items[i] = fmtVal(item)
			}
			return "(" + strings.Join(items, ", ") + ")"
		}

		// Map access: $name.key or $name[0].key
		if val.IsMap() {
			idx := 0
			if indexStr != "" {
				idx, _ = strconv.Atoi(indexStr)
			}
			if idx >= 0 && idx < len(val.maps) {
				row := val.maps[idx]
				if key != "" {
					if v, ok := row[key]; ok {
						return fmtVal(v)
					}
					return match
				}
				// No key: return sorted JSON
				b, _ := json.Marshal(sortMapKeys(row))
				return string(b)
			}
		}

		return match
	})
}

// ForLoop represents a parsed for-in loop
type ForLoop struct {
	VarName string   // loop variable name (e.g., "row")
	IterVar string   // iterable variable name (e.g., "rows")
	Body    []string // SQL statements to execute for each iteration
}

// ParseForLoop parses a for-in loop from lines
// Format: \for <var> in $<iter> { ... }
func ParseForLoop(lines []string) (*ForLoop, int, error) {
	// Match "\for <var> in $<iter> {"
	forPattern := regexp.MustCompile(`^\\?for\s+(\w+)\s+in\s+\$(\w+)\s*\{`)
	loop := &ForLoop{}
	startIdx := -1
	braceDepth := 0

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if startIdx < 0 {
			m := forPattern.FindStringSubmatch(trimmed)
			if m != nil {
				startIdx = i
				loop.VarName = m[1]
				loop.IterVar = m[2]
				braceDepth = 1
				// Check if body/closing brace is on same line after {
				afterBrace := trimmed[strings.Index(trimmed, "{")+1:]
				afterBrace = strings.TrimSpace(afterBrace)
				// Count braces in the remainder of the same line
				braceDepth += strings.Count(afterBrace, "{") - strings.Count(afterBrace, "}")
				if braceDepth <= 0 {
					// Loop closes on the same line
					if idx := strings.LastIndex(afterBrace, "}"); idx > 0 {
						body := strings.TrimSpace(afterBrace[:idx])
						if body != "" {
							loop.Body = append(loop.Body, body)
						}
					}
					return loop, i + 1, nil
				}
				if afterBrace != "" {
					loop.Body = append(loop.Body, afterBrace)
				}
				continue
			}
			continue
		}

		// Count braces
		braceDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
		if braceDepth <= 0 {
			// Check for content before closing brace
			beforeClose := trimmed
			if idx := strings.LastIndex(trimmed, "}"); idx > 0 {
				beforeClose = strings.TrimSpace(trimmed[:idx])
			} else {
				beforeClose = ""
			}
			if beforeClose != "" {
				loop.Body = append(loop.Body, beforeClose)
			}
			return loop, i + 1, nil
		}

		loop.Body = append(loop.Body, trimmed)
	}

	if startIdx >= 0 {
		return nil, 0, fmt.Errorf("unclosed for loop")
	}
	return nil, 0, nil
}

// ExecuteForLoop executes a for-in loop, substituting the loop variable
func (s *VarStore) ExecuteForLoop(loop *ForLoop, executor *Executor) error {
	iterVal, ok := s.vars[loop.IterVar]
	if !ok {
		return fmt.Errorf("variable $%s not found", loop.IterVar)
	}

	var items []map[string]any
	switch {
	case iterVal.IsMap():
		items = iterVal.maps
	case iterVal.IsList():
		// Convert list items to maps for uniform access
		for i, item := range iterVal.list {
			if m, ok := item.(map[string]any); ok {
				items = append(items, m)
			} else {
				items = append(items, map[string]any{
					"index": i,
					"value": item,
				})
			}
		}
	default:
		return fmt.Errorf("variable $%s is not iterable", loop.IterVar)
	}

	for _, item := range items {
		// Set loop variable to current item
		s.vars[loop.VarName] = &VarValue{maps: []map[string]any{sortMapKeys(item)}}

		// Execute each body statement
		for _, stmt := range loop.Body {
			expanded := s.ExpandVars(stmt)
			result, err := executor.ExecSQL(expanded)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			PrintResult(result)
		}
	}

	// Clean up loop variable
	delete(s.vars, loop.VarName)
	return nil
}

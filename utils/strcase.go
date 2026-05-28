package utils

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// abbreviations stores registered abbreviations for ToSnakeCase.
// Key is the abbreviation as it appears in Go field names (e.g. "PRs", "ID", "URL"),
// Value is the desired snake_case form (e.g. "prs", "id", "url").
var (
	abbreviations   map[string]string
	abbreviationsMu sync.RWMutex
)

func init() {
	abbreviations = DefaultAbbreviations()
}

// DefaultAbbreviations returns the default set of common abbreviations.
func DefaultAbbreviations() map[string]string {
	return map[string]string{
		"ID":    "id",
		"IDs":   "ids",
		"URL":   "url",
		"URLs":  "urls",
		"API":   "api",
		"HTTP":  "http",
		"HTTPS": "https",
		"SSH":   "ssh",
		"SQL":   "sql",
		"JSON":  "json",
		"XML":   "xml",
		"HTML":  "html",
		"CSS":   "css",
		"PR":    "pr",
		"PRs":   "prs",
		"IP":    "ip",
		"IPs":   "ips",
		"TCP":   "tcp",
		"UDP":   "udp",
		"DNS":   "dns",
		"CPU":   "cpu",
		"GPU":   "gpu",
		"RAM":   "ram",
		"IO":    "io",
	}
}

// RegisterAbbreviations adds custom abbreviations for ToSnakeCase.
// Abbreviations are merged with the defaults; custom ones override defaults.
// For example, RegisterAbbreviations(map[string]string{"CRM": "crm"})
// makes "CRMSystem" convert to "crm_system".
func RegisterAbbreviations(abbrs map[string]string) {
	abbreviationsMu.Lock()
	defer abbreviationsMu.Unlock()
	for k, v := range abbrs {
		abbreviations[k] = v
	}
}

// sortedAbbreviationKeys returns abbreviation keys sorted by length descending
// so that longer matches are tried first (e.g. "HTTPS" before "HTTP").
func sortedAbbreviationKeys() []string {
	abbreviationsMu.RLock()
	defer abbreviationsMu.RUnlock()
	keys := make([]string, 0, len(abbreviations))
	for k := range abbreviations {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i]) > len(keys[j])
	})
	return keys
}

// ToTitleCase converts a string to title case
// It capitalizes the first letter of each word
func ToTitleCase(word string) string {
	caser := cases.Title(language.Und)
	return caser.String(word)
	// if word == "" {
	// 	return ""
	// }
	// runes := []rune(word)
	// runes[0] = unicode.ToTitle(runes[0])
	// return string(runes)
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

// ToSnakeCase converts camelCase or PascalCase to snake_case.
// Registered abbreviations are kept together (e.g. "PRs" -> "prs" not "p_rs").
func ToSnakeCase(word string) string {
	// Replace abbreviations with protected lowercase forms.
	// The \x01 delimiters act as word boundaries for underscore insertion.
	keys := sortedAbbreviationKeys()
	for _, abbr := range keys {
		abbreviationsMu.RLock()
		snake := abbreviations[abbr]
		abbreviationsMu.RUnlock()
		protected := "\x01" + snake + "\x01"
		word = strings.ReplaceAll(word, abbr, protected)
	}

	result := toSnakeCaseRaw(word)

	// \x01 characters become underscores in the raw conversion,
	// but may produce double underscores. Clean up the result.
	result = strings.ReplaceAll(result, "\x01", "_")
	result = strings.ReplaceAll(result, "__", "_")
	result = strings.Trim(result, "_")
	return result
}

// toSnakeCaseRaw does the actual camelCase/PascalCase to snake_case conversion.
func toSnakeCaseRaw(word string) string {
	var prev []rune
	b := strings.Builder{}
	prevUp, currUp := false, false
	for i, letter := range word {
		if currUp = unicode.IsUpper(letter); currUp {
			letter = unicode.ToLower(letter)
		}
		if prevUp { // cache to variable named prev
			if n := len(prev); n > 0 && !currUp {
				prev = slices.Insert(prev, n-1, '_')
			}
			prev = append(prev, letter)
		} else { // write to the result and clear prev
			b.WriteString(string(prev))
			if currUp && i > 0 {
				b.WriteRune('_')
			}
			b.WriteRune(letter)
			prev = prev[:0]
		}
		prevUp = currUp
	}
	b.WriteString(string(prev))
	return b.String()
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

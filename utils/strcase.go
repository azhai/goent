package utils

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// abbrEntry is a single abbreviation mapping used by ToSnakeCase.
type abbrEntry struct {
	// word is the abbreviation as it appears in Go identifiers ("ID", "URLs").
	word string
	// snake is the desired snake_case form ("id", "urls").
	snake string
}

var (
	// abbreviations holds the registered abbreviations.
	// It is only read/written under abbreviationsMu.
	abbreviations   = DefaultAbbreviations()
	abbreviationsMu sync.RWMutex

	// sortedAbbreviations holds an immutable []abbrEntry sorted by word length
	// descending (longest match first). It is replaced, never mutated, so
	// readers need no lock at all — ToSnakeCase is on the hot path of every
	// struct/table mapping.
	sortedAbbreviations atomic.Value // []abbrEntry
)

func init() {
	rebuildSortedAbbreviations()
}

// DefaultAbbreviations returns the default set of common abbreviations.
func DefaultAbbreviations() map[string]string {
	return map[string]string{
		"API":   "api",
		"CPU":   "cpu",
		"CSS":   "css",
		"DNS":   "dns",
		"GPU":   "gpu",
		"HTML":  "html",
		"HTTP":  "http",
		"HTTPS": "https",
		"ID":    "id",
		"IDs":   "ids",
		"IO":    "io",
		"IP":    "ip",
		"IPs":   "ips",
		"JSON":  "json",
		"RAM":   "ram",
		"SQL":   "sql",
		"SSH":   "ssh",
		"TCP":   "tcp",
		"UDP":   "udp",
		"URL":   "url",
		"URLs":  "urls",
		"XML":   "xml",
	}
}

// RegisterAbbreviations adds custom abbreviations for ToSnakeCase.
// Abbreviations are merged with the defaults; custom ones override defaults.
// For example, RegisterAbbreviations(map[string]string{"CRM": "crm"})
// makes "CRMSystem" convert to "crm_system".
//
// The list is published atomically, so concurrent calls to ToSnakeCase observe
// either the old or the new set — never a partially updated one.
func RegisterAbbreviations(abbrs map[string]string) {
	abbreviationsMu.Lock()
	for k, v := range abbrs {
		if k == "" {
			continue
		}
		if v == "" {
			v = strings.ToLower(k)
		}
		abbreviations[k] = v
	}
	abbreviationsMu.Unlock()
	rebuildSortedAbbreviations()
}

// LoadAbbreviationsFromFile loads custom abbreviations from a JSON file.
// The file should contain a JSON object mapping Go abbreviation names to
// their snake_case forms, e.g. {"ETag": "etag", "CRM": "crm"}.
// If the file does not exist, no error is returned.
func LoadAbbreviationsFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return LoadAbbreviationsFromData(data)
}

// LoadAbbreviationsFromData loads custom abbreviations from JSON bytes.
// The data should contain a JSON object mapping Go abbreviation names to
// their snake_case forms, e.g. {"ETag": "etag", "CRM": "crm"}.
func LoadAbbreviationsFromData(data []byte) error {
	var abbrs map[string]string
	if err := json.Unmarshal(data, &abbrs); err != nil {
		return err
	}
	RegisterAbbreviations(abbrs)
	return nil
}

// rebuildSortedAbbreviations snapshots the abbreviation map into an immutable,
// length-descending sorted slice and publishes it atomically.
func rebuildSortedAbbreviations() {
	abbreviationsMu.RLock()
	entries := make([]abbrEntry, 0, len(abbreviations))
	for k, v := range abbreviations {
		if k == "" {
			continue
		}
		if v == "" {
			v = strings.ToLower(k)
		}
		entries = append(entries, abbrEntry{word: k, snake: v})
	}
	abbreviationsMu.RUnlock()

	// Longest first so "HTTPS" wins over "HTTP".
	sort.Slice(entries, func(i, j int) bool {
		if len(entries[i].word) != len(entries[j].word) {
			return len(entries[i].word) > len(entries[j].word)
		}
		return entries[i].word < entries[j].word
	})
	sortedAbbreviations.Store(entries)
}

// ToTitleCase converts a string to title case
// It capitalizes the first letter of each word
//
// A cases.Caser is stateful and must not be shared between goroutines,
// so a new one is built per call.
func ToTitleCase(word string) string {
	caser := cases.Title(language.Und)
	return caser.String(word)
}

// ToCamelCase converts snake_case to CamelCase
func ToCamelCase(s string) string {
	parts := strings.Split(s, "_")
	last := len(parts) - 1
	for i, word := range parts {
		if i == last && strings.ToLower(word) == "id" {
			parts[i] = "ID"
			continue
		}
		if word == "" {
			continue
		}
		head, size := utf8.DecodeRuneInString(word)
		// DecodeRuneInString never returns size 0 for a non-empty string,
		// but stay defensive: never slice inside a multi-byte rune.
		if size == 0 {
			continue
		}
		parts[i] = string(unicode.ToUpper(head)) + strings.ToLower(word[size:])
	}
	return strings.Join(parts, "")
}

// ToSnakeCase converts camelCase or PascalCase to snake_case.
// Registered abbreviations are kept together (e.g. "PRs" -> "prs" not "p_rs").
//
// An abbreviation only matches on a word boundary, so "Identity" stays
// "identity" instead of being split into "id_entity".
func ToSnakeCase(word string) string {
	if word == "" {
		return ""
	}
	abbrs, _ := sortedAbbreviations.Load().([]abbrEntry)
	b := strings.Builder{}
	b.Grow(len(word) + 4)

	// Iterating over the string by index keeps this allocation free:
	// no []rune conversion is needed for the look-behind / look-ahead checks.
	var prev rune
	for i := 0; i < len(word); {
		r, size := utf8.DecodeRuneInString(word[i:])
		next := i + size
		if !unicode.IsUpper(r) {
			b.WriteRune(unicode.ToLower(r))
			prev, i = r, next
			continue
		}
		// Upper case rune: it may open a new word.
		if i > 0 {
			nr, hasNext := decodeFirstRune(word[next:])
			if startsWord(prev, nr, hasNext) {
				b.WriteRune('_')
			}
		}
		if end, snake := matchAbbreviation(abbrs, word, i); end > 0 {
			b.WriteString(snake)
			prev, _ = utf8.DecodeLastRuneInString(word[:end])
			i = end
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		prev, i = r, next
	}

	result := b.String()
	if !strings.Contains(result, "__") {
		return strings.Trim(result, "_")
	}
	return collapseUnderscores(result)
}

// decodeFirstRune returns the first rune of s and whether s is non-empty.
func decodeFirstRune(s string) (rune, bool) {
	if s == "" {
		return 0, false
	}
	r, _ := utf8.DecodeRuneInString(s)
	return r, true
}

// startsWord reports whether an upper case rune begins a new word.
// A new word starts after a lower case letter or a digit ("aB" -> "a_b",
// "v2X" -> "v2_x"), or when an upper case run is followed by a lower case
// letter ("XMLParser" -> "xml_parser").
func startsWord(prev, next rune, hasNext bool) bool {
	if unicode.IsLower(prev) || unicode.IsDigit(prev) {
		return true
	}
	return hasNext && unicode.IsLower(next)
}

// matchAbbreviation tries to match a registered abbreviation at byte offset i.
// It returns the end offset of the match and the snake_case replacement,
// or (0, "") when there is no match.
//
// A match is only accepted when it ends on a word boundary: at the end of the
// identifier or right before an upper case rune. This keeps "ID" from matching
// inside "Identity".
func matchAbbreviation(abbrs []abbrEntry, word string, i int) (int, string) {
	rest := word[i:]
	for _, entry := range abbrs {
		if !strings.HasPrefix(rest, entry.word) {
			continue
		}
		end := i + len(entry.word)
		if end < len(word) {
			if r, _ := utf8.DecodeRuneInString(word[end:]); unicode.IsLower(r) {
				continue
			}
		}
		return end, entry.snake
	}
	return 0, ""
}

// collapseUnderscores merges runs of underscores and trims the ends.
func collapseUnderscores(s string) string {
	b := strings.Builder{}
	b.Grow(len(s))
	pending := false
	for _, r := range s {
		if r == '_' {
			pending = true
			continue
		}
		if pending {
			b.WriteRune('_')
			pending = false
		}
		b.WriteRune(r)
	}
	return strings.Trim(b.String(), "_")
}

// ToSingular converts a plural name to its singular form.
func ToSingular(name string) string {
	if strings.HasSuffix(name, "ies") && len(name) > 3 {
		return name[:len(name)-3] + "y"
	}
	if strings.HasSuffix(name, "ses") && len(name) > 3 {
		return name[:len(name)-2]
	}
	// Words like "status"/"bus" end in "s" but are already singular.
	if strings.HasSuffix(name, "us") || strings.HasSuffix(name, "is") {
		return name
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
		// "category" -> "categories", but "day" -> "days".
		if isVowel(rune(name[len(name)-2])) {
			return name + "s"
		}
		return name[:len(name)-1] + "ies"
	}
	if strings.HasSuffix(name, "s") || strings.HasSuffix(name, "x") ||
		strings.HasSuffix(name, "z") || strings.HasSuffix(name, "ch") ||
		strings.HasSuffix(name, "sh") {
		return name + "es"
	}
	return name + "s"
}

// isVowel reports whether r is an ASCII vowel, in either case.
func isVowel(r rune) bool {
	switch unicode.ToLower(r) {
	case 'a', 'e', 'i', 'o', 'u':
		return true
	}
	return false
}

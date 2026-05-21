package utils

import (
	"slices"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

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

// ToSnakeCase converts camelCase or PascalCase to snake_case
func ToSnakeCase(word string) string {
	var prev []rune
	b := strings.Builder{}
	prevUp, currUp := false, false
	for i, letter := range word {
		if currUp = unicode.IsUpper(letter); currUp {
			letter = unicode.ToLower(letter)
		}
		if prevUp { // cache to varibale named prev
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

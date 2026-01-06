package utils

import (
	"strings"
	"unicode"
)

// TableNamePattern is the default name patterning for mapping struct to table
func TableNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	name = ToSnakeCase(name)
	if name[len(name)-1] != 's' {
		name += "s"
	}
	return name
}

// ColumnNamePattern is the default name patterning for mapping struct fields to table columns
func ColumnNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	return ToSnakeCase(name)
}

func ToSnakeCase(name string) string {
	if len(name) == 0 {
		return name
	}

	result := strings.Builder{}
	for i := 0; i < len(name); i++ {
		letter := rune(name[i])
		if unicode.IsUpper(letter) {
			letter = unicode.ToLower(letter)
			if i > 0 {
				prevLetter := rune(name[i-1])
				if unicode.IsLower(prevLetter) {
					result.WriteRune('_')
				} else if i+1 < len(name) && unicode.IsLower(rune(name[i+1])) {
					result.WriteRune('_')
				}
			}
		}
		result.WriteRune(letter)
	}

	return result.String()
}

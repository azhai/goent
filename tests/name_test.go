package goent_test

import (
	"testing"

	"github.com/azhai/goent/utils"
)

func TestToSnakeCase(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single lowercase letter",
			input:    "a",
			expected: "a",
		},
		{
			name:     "single uppercase letter",
			input:    "A",
			expected: "a",
		},
		{
			name:     "two letters camelCase",
			input:    "aB",
			expected: "a_b",
		},
		{
			name:     "two letters uppercase",
			input:    "AB",
			expected: "ab",
		},
		{
			name:     "single word lowercase",
			input:    "user",
			expected: "user",
		},
		{
			name:     "single word uppercase",
			input:    "USER",
			expected: "user",
		},
		{
			name:     "all uppercase",
			input:    "URL",
			expected: "url",
		},
		{
			name:     "all uppercase longer",
			input:    "HTTP",
			expected: "http",
		},
		{
			name:     "simple camelCase",
			input:    "userName",
			expected: "user_name",
		},
		{
			name:     "camelCase with multiple words",
			input:    "getUserName",
			expected: "get_user_name",
		},
		{
			name:     "camelCase ending with uppercase",
			input:    "userID",
			expected: "user_id",
		},
		{
			name:     "PascalCase",
			input:    "UserName",
			expected: "user_name",
		},
		{
			name:     "camelCase with consecutive uppercase",
			input:    "parseXMLData",
			expected: "parse_xml_data",
		},
		{
			name:     "PascalCase with consecutive uppercase",
			input:    "XMLParser",
			expected: "xml_parser",
		},
		{
			name:     "mixed case with numbers",
			input:    "user123",
			expected: "user123",
		},
		{
			name:     "already snake_case",
			input:    "user_name",
			expected: "user_name",
		},
		// Abbreviation-aware cases
		{
			name:     "abbreviation PRs kept together",
			input:    "OpenPRsCount",
			expected: "open_prs_count",
		},
		{
			name:     "abbreviation ID kept together",
			input:    "UserID",
			expected: "user_id",
		},
		{
			name:     "abbreviation HTTPS kept together",
			input:    "HTTPSEndpoint",
			expected: "https_endpoint",
		},
		{
			name:     "abbreviation XML kept together",
			input:    "parseXMLData",
			expected: "parse_xml_data",
		},
		{
			name:     "abbreviation URL at start",
			input:    "URLPath",
			expected: "url_path",
		},
		{
			name:     "abbreviation IDs plural",
			input:    "UserIDs",
			expected: "user_ids",
		},
		{
			name:     "abbreviation API in middle",
			input:    "RestAPIHandler",
			expected: "rest_api_handler",
		},
	}

	for _, cas := range testCases {
		t.Run(cas.name, func(t *testing.T) {
			result := utils.ToSnakeCase(cas.input)
			if result != cas.expected {
				t.Errorf("ToSnakeCase(%q) got %q, BUT want %q", cas.input, result, cas.expected)
			}
		})
	}
}

func TestRegisterAbbreviations(t *testing.T) {
	// Register a custom abbreviation
	utils.RegisterAbbreviations(map[string]string{
		"CRM": "crm",
	})

	cases := []struct {
		input    string
		expected string
	}{
		{"CRMSystem", "crm_system"},
		{"OpenCRMPortal", "open_crm_portal"},
	}

	for _, cas := range cases {
		result := utils.ToSnakeCase(cas.input)
		if result != cas.expected {
			t.Errorf("ToSnakeCase(%q) got %q, BUT want %q", cas.input, result, cas.expected)
		}
	}
}

package utils_test

import (
	"sync"
	"testing"

	"github.com/azhai/goent/utils"
)

func TestToSnakeCase_Basics(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"a", "a"},
		{"A", "a"},
		{"aB", "a_b"},
		{"AB", "ab"},
		{"ABc", "a_bc"},
		{"user", "user"},
		{"USER", "user"},
		{"UserName", "user_name"},
		{"userName", "user_name"},
		{"user_name", "user_name"},
		{"user123", "user123"},
		{"User2Name", "user2_name"},
		{"XMLParser", "xml_parser"},
		{"parseXMLData", "parse_xml_data"},
		{"HTTPServer", "http_server"},
		{"HTTPSConnection", "https_connection"},
		{"A_B", "a_b"},
		{"a___b", "a_b"},
	}
	for _, c := range cases {
		if got := utils.ToSnakeCase(c.input); got != c.want {
			t.Errorf("ToSnakeCase(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// TestToSnakeCase_AbbreviationBoundary guards the regression where "ID" matched
// inside "Identity" and produced "id_entity".
func TestToSnakeCase_AbbreviationBoundary(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"Identity", "identity"},
		{"IdentityCard", "identity_card"},
		{"Item", "item"},
		{"Idle", "idle"},
		{"IDLE", "idle"},
		{"ID", "id"},
		{"IDCard", "id_card"},
		{"UserID", "user_id"},
		{"UserIDs", "user_ids"},
		{"URLPath", "url_path"},
		{"URLs", "urls"},
		{"IPs", "ips"},
		{"APIHandler", "api_handler"},
		{"RestAPIHandler", "rest_api_handler"},
		{"MyURLs", "my_urls"},
		{"IDsAndMore", "ids_and_more"},
	}
	for _, c := range cases {
		if got := utils.ToSnakeCase(c.input); got != c.want {
			t.Errorf("ToSnakeCase(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// TestToSnakeCase_Unicode makes sure non-ASCII identifiers are not mangled.
func TestToSnakeCase_Unicode(t *testing.T) {
	if got, want := utils.ToSnakeCase("用户Name"), "用户_name"; got != want {
		t.Errorf("ToSnakeCase(用户Name) = %q, want %q", got, want)
	}
	if got, want := utils.ToSnakeCase("Éclair"), "éclair"; got != want {
		t.Errorf("ToSnakeCase(Éclair) = %q, want %q", got, want)
	}
}

func TestRegisterAbbreviations_Override(t *testing.T) {
	// CRMSystem has no registered abbreviation by default.
	if got, want := utils.ToSnakeCase("CRMSystem"), "crm_system"; got != want {
		t.Fatalf("precondition failed: ToSnakeCase(CRMSystem) = %q, want %q", got, want)
	}
	utils.RegisterAbbreviations(map[string]string{"CRM": "crm", "ETag": "etag"})
	if got, want := utils.ToSnakeCase("ETagValue"), "etag_value"; got != want {
		t.Errorf("ToSnakeCase(ETagValue) = %q, want %q", got, want)
	}
	if got, want := utils.ToSnakeCase("ETagger"), "e_tagger"; got != want {
		t.Errorf("ToSnakeCase(ETagger) = %q, want %q (abbreviation must not match inside a word)", got, want)
	}
	// Empty key / empty value must not break the matcher.
	utils.RegisterAbbreviations(map[string]string{"": "ignored", "SKU": ""})
	if got, want := utils.ToSnakeCase("SKUCode"), "sku_code"; got != want {
		t.Errorf("ToSnakeCase(SKUCode) = %q, want %q", got, want)
	}
}

func TestLoadAbbreviationsFromData(t *testing.T) {
	if err := utils.LoadAbbreviationsFromData([]byte(`{"JWT":"jwt"}`)); err != nil {
		t.Fatalf("LoadAbbreviationsFromData: %v", err)
	}
	if got, want := utils.ToSnakeCase("JWTToken"), "jwt_token"; got != want {
		t.Errorf("ToSnakeCase(JWTToken) = %q, want %q", got, want)
	}
	if err := utils.LoadAbbreviationsFromData([]byte(`not json`)); err == nil {
		t.Error("LoadAbbreviationsFromData should reject invalid JSON")
	}
}

// TestToSnakeCase_Concurrent exercises the lock-free abbreviation snapshot.
// Run with -race to catch unsynchronized map/slice access.
func TestToSnakeCase_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = utils.ToSnakeCase("UserIDAndURLs")
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				utils.RegisterAbbreviations(map[string]string{"XYZ": "xyz"})
			}
		}()
	}
	wg.Wait()
}

func TestToCamelCase(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"user", "User"},
		{"user_name", "UserName"},
		{"user_id", "UserID"},
		{"id", "ID"},
		{"url_path", "UrlPath"},
		// The first rune is multi-byte: a byte-wise upper case would corrupt it.
		{"éclair_name", "ÉclairName"},
		{"a_b_c", "ABC"},
	}
	for _, c := range cases {
		if got := utils.ToCamelCase(c.input); got != c.want {
			t.Errorf("ToCamelCase(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestToPlural(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"category", "categories"},
		{"user", "users"},
		{"status", "statuses"},
		// Vowel before the trailing "y": simple +s, not +ies.
		{"day", "days"},
		{"boy", "boys"},
		{"key", "keys"},
		{"box", "boxes"},
		{"class", "classes"},
		{"dish", "dishes"},
		{"church", "churches"},
	}
	for _, c := range cases {
		if got := utils.ToPlural(c.input); got != c.want {
			t.Errorf("ToPlural(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestToSingular(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"categories", "category"},
		{"classes", "class"},
		{"users", "user"},
		{"keys", "key"},
		// Already singular: "status"/"analysis" must not lose their trailing s.
		{"status", "status"},
		{"bus", "bus"},
		{"analysis", "analysis"},
		{"address", "address"},
	}
	for _, c := range cases {
		if got := utils.ToSingular(c.input); got != c.want {
			t.Errorf("ToSingular(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestToTitleCase(t *testing.T) {
	if got, want := utils.ToTitleCase("hello world"), "Hello World"; got != want {
		t.Errorf("ToTitleCase(hello world) = %q, want %q", got, want)
	}
	if got := utils.ToTitleCase(""); got != "" {
		t.Errorf("ToTitleCase(\"\") = %q, want empty", got)
	}
}

func BenchmarkToSnakeCase(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = utils.ToSnakeCase("UserIDAndURLs")
	}
}

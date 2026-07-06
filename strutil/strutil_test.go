package strutil

import "testing"

func TestCapitalizeFirstLetter(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want string
	}{
		{
			name: "empty string",
			arg:  "",
			want: "",
		},
		{
			name: "single rune",
			arg:  "s",
			want: "S",
		},
		{
			name: "normal word",
			arg:  "sUsPend",
			want: "Suspend",
		},
		{
			name: "with number",
			arg:  "numb3r",
			want: "Numb3r",
		},
		{
			name: "two words",
			arg:  "two words",
			want: "Two words",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CapitalizeFirstRune(tt.arg); got != tt.want {
				t.Errorf("CapitalizeFirstRune() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_WildCardToRegexp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
		wanted  string
	}{
		{
			name:    "main*",
			pattern: "main*",
			wanted:  "main.*",
		},
		{
			name:    "*.txt",
			pattern: "*.txt",
			wanted:  ".*\\.txt",
		},
		{
			name:    "?_main*.txt",
			pattern: "?_main*.txt",
			wanted:  "._main.*\\.txt",
		},
		{
			name:    "literal dot and slash",
			pattern: "a.b/c",
			wanted:  "a\\.b/c",
		},
		{
			name:    "empty pattern",
			pattern: "",
			wanted:  "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := WildCardToRegexp(tt.pattern); got != tt.wanted {
				t.Errorf("wildCardToRegexp() = %v, want %v", got, tt.wanted)
			}
		})
	}
}

// TestHumanizeBytes verifies the SI-style humanizer for byte counts.
//
// A value exactly equal to a unit boundary (1024, 1<<20, ...) renders in
// the larger unit ("1.0K", not "1024").
func TestHumanizeBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		b    int64
		want string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"bytes_under_kib", 512, "512"},
		{"just_under_1k", 1023, "1023"},
		{"one_kib_boundary", 1 << 10, "1.0K"},
		{"one_mib_boundary", 1 << 20, "1.0M"},
		{"one_gib_boundary", 1 << 30, "1.0G"},
		{"one_tib_boundary", 1 << 40, "1.0T"},
		{"just_over_1k", 1025, "1.0K"},
		{"fractional_kib", 1536, "1.5K"},
		{"fractional_mib", 5 * (1 << 20), "5.0M"},
		{"negative_byte_count", -1, "-1"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := HumanizeBytes(tc.b); got != tc.want {
				t.Errorf("HumanizeBytes(%d) = %q, want %q", tc.b, got, tc.want)
			}
		})
	}
}

// TestMatchFromStartToEnd verifies the regex anchor helper.
func TestMatchFromStartToEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"plain", "abc", "^abc$"},
		{"with_wildcard", "ab.*", "^ab.*$"},
		{"empty", "", "^$"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := MatchFromStartToEnd(tc.in); got != tc.want {
				t.Errorf("MatchFromStartToEnd(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestAddNewLineFlag verifies the (?s) prefix is added so . matches \n.
func TestAddNewLineFlag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"plain", "abc", "(?s)abc"},
		{"empty", "", "(?s)"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AddNewLineFlag(tc.in); got != tc.want {
				t.Errorf("AddNewLineFlag(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestJSON verifies the JSON helper marshals simple values without error.
// We do not assert the exact byte output (map ordering is not stable).
func TestJSON(t *testing.T) {
	t.Parallel()
	if got := JSON("hello"); got != `"hello"` {
		t.Errorf("JSON(string) = %q, want %q", got, `"hello"`)
	}
	if got := JSON(42); got != "42" {
		t.Errorf("JSON(42) = %q, want %q", got, "42")
	}
	// nil marshals to "null".
	if got := JSON(nil); got != "null" {
		t.Errorf("JSON(nil) = %q, want %q", got, "null")
	}
}

func TestTrimQuotes(t *testing.T) {
	testCases := []struct {
		in   string
		want string
	}{
		{`"abc"`, "abc"},
		{`'abc'`, "abc"},
		{`""abc""`, "abc"},
		{`abc`, "abc"},
		{`"abc`, `"abc`},   // unbalanced quotes are kept
		{`"abc'`, `"abc'`}, // mismatched pair is kept
		{`""`, ""},
		{`"`, `"`},
		{"", ""},
	}
	for _, tc := range testCases {
		if got := TrimQuotes(tc.in); got != tc.want {
			t.Errorf("TrimQuotes(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

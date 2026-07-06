package run

import (
	"reflect"
	"testing"
)

// TestShellquoteSplit exercises the POSIX-ish tokenizer: plain fields,
// single/double quotes, backslash escapes and the error paths. The old
// strings.Fields implementation broke every quoted argument (a key with a
// space became three tokens, so rm targeted the wrong keys) and its
// unterminated-quote error path was dead code.
func TestShellquoteSplit(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"plain fields", "cp a.txt s3://bucket/a.txt", []string{"cp", "a.txt", "s3://bucket/a.txt"}},
		{"collapses runs of whitespace", "  ls \t  s3://b  ", []string{"ls", "s3://b"}},
		{"double-quoted spaces", `rm "s3://b/file with spaces"`, []string{"rm", "s3://b/file with spaces"}},
		{"single-quoted spaces", `rm 's3://b/file with spaces'`, []string{"rm", "s3://b/file with spaces"}},
		{"quotes join adjacent text", `cp pre"fix mid"post dst`, []string{"cp", "prefix midpost", "dst"}},
		{"escaped space", `rm s3://b/a\ b`, []string{"rm", "s3://b/a b"}},
		{"escaped quote outside quotes", `echo \"hi\"`, []string{"echo", `"hi"`}},
		{"escaped quote inside double quotes", `echo "a \"b\" c"`, []string{"echo", `a "b" c`}},
		{"backslash literal inside double quotes", `echo "a\tb"`, []string{"echo", `a\tb`}},
		{"escaped backslash inside double quotes", `echo "a\\b"`, []string{"echo", `a\b`}},
		{"single quotes are literal", `echo 'a\ b"c'`, []string{"echo", `a\ b"c`}},
		{"empty double-quoted field", `cp "" dst`, []string{"cp", "", "dst"}},
		{"empty single-quoted field", `cp '' dst`, []string{"cp", "", "dst"}},
		{"empty line", "", nil},
		{"whitespace only", "   \t ", nil},
	}
	for _, c := range cases {
		got, err := shellquoteSplit(c.in)
		if err != nil {
			t.Errorf("%s: shellquoteSplit(%q) error: %v", c.name, c.in, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: shellquoteSplit(%q) = %#v, want %#v", c.name, c.in, got, c.want)
		}
	}
}

// TestShellquoteSplitErrors verifies the malformed-line error paths.
func TestShellquoteSplitErrors(t *testing.T) {
	for _, in := range []string{
		`rm "s3://b/unterminated`,
		`rm 's3://b/unterminated`,
		`rm "closed" "open`,
		`rm trailing\`,
	} {
		if _, err := shellquoteSplit(in); err == nil {
			t.Errorf("shellquoteSplit(%q) = nil error, want unterminated-quote/backslash error", in)
		}
	}
}

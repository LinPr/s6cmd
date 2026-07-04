package storage

import (
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/LinPr/s6cmd/strutil"
)

// TestNewStorageURL verifies that NewStorageURL parses local paths, bare
// buckets, object keys, prefixes, and versioned URLs into the expected
// StorageURL fields, and rejects schemes that are not s3://.
func TestNewStorageURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    *StorageURL
		wantErr bool
	}{
		{
			name:  "local_path",
			input: "/tmp/file.txt",
			want: &StorageURL{
				Type:      localObject,
				Scheme:    "",
				Path:      "/tmp/file.txt",
				Prefix:    "/tmp/file.txt",
				Delimiter: "/",
			},
		},
		{
			name:  "local_relative_path",
			input: "dir/file.txt",
			want: &StorageURL{
				Type:      localObject,
				Scheme:    "",
				Path:      "dir/file.txt",
				Prefix:    "dir/file.txt",
				Delimiter: "/",
			},
		},
		{
			name:  "bucket_only",
			input: "s3://bucket",
			want: &StorageURL{
				Type:      remoteObject,
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "",
				Prefix:    "",
				Delimiter: "/",
			},
		},
		{
			name:  "bucket_and_key",
			input: "s3://bucket/key",
			want: &StorageURL{
				Type:      remoteObject,
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "key",
				Prefix:    "key",
				Delimiter: "/",
			},
		},
		{
			name:  "bucket_and_prefix",
			input: "s3://bucket/prefix/",
			want: &StorageURL{
				Type:      remoteObject,
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "prefix/",
				Prefix:    "prefix/",
				Delimiter: "/",
			},
		},
		{
			name:  "bucket_and_nested_key",
			input: "s3://bucket/a/b/c",
			want: &StorageURL{
				Type:      remoteObject,
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "a/b/c",
				Prefix:    "a/b/c",
				Delimiter: "/",
			},
		},
		{
			name:  "wildcard_key",
			input: "s3://bucket/logs/*.log",
			want: &StorageURL{
				Type:      remoteObject,
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "logs/*.log",
				Prefix:    "logs/",
				Delimiter: "",
			},
		},
		{
			name:    "error_if_no_bucket",
			input:   "s3://",
			wantErr: true,
		},
		{
			name:    "error_if_bucket_has_wildcard",
			input:   "s3://a*b",
			wantErr: true,
		},
		{
			name:    "error_if_unsupported_scheme",
			input:   "gs://bucket/key",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewStorageURL(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewStorageURL() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got.Type != tc.want.Type ||
				got.Scheme != tc.want.Scheme ||
				got.Bucket != tc.want.Bucket ||
				got.Path != tc.want.Path ||
				got.Prefix != tc.want.Prefix ||
				got.Delimiter != tc.want.Delimiter {
				t.Errorf("NewStorageURL() got = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestNewStorageURLVersionID confirms that ?versionId=xxx is stripped from
// the key and exposed via VersionID.
func TestNewStorageURLVersionID(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/key?versionId=abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.VersionID != "abc123" {
		t.Errorf("VersionID = %q, want %q", u.VersionID, "abc123")
	}
	if u.Path != "key" {
		t.Errorf("Path = %q, want %q", u.Path, "key")
	}
	if u.Bucket != "bucket" {
		t.Errorf("Bucket = %q, want %q", u.Bucket, "bucket")
	}
}

// TestNewStorageURLWithVersionOption ensures the WithVersion option still
// overrides anything parsed from the URL itself.
func TestNewStorageURLWithVersionOption(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/key", WithVersion("opt-version"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.VersionID != "opt-version" {
		t.Errorf("VersionID = %q, want %q", u.VersionID, "opt-version")
	}
}

// TestNewStorageURLWithAllVersions exercises the WithAllVersions option and
// the IsVersioned helper.
func TestNewStorageURLWithAllVersions(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/key", WithAllVersions(true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !u.AllVersions {
		t.Errorf("AllVersions = false, want true")
	}
	if !u.IsVersioned() {
		t.Errorf("IsVersioned() = false, want true")
	}

	plain, err := NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plain.IsVersioned() {
		t.Errorf("IsVersioned() = true, want false")
	}
}

// TestStorageURLIsRemote verifies IsRemote for both local and remote URLs.
func TestStorageURLIsRemote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"s3://bucket", true},
		{"s3://bucket/key", true},
		{"/local/path", false},
		{"relative/path", false},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.IsRemote(); got != tc.want {
			t.Errorf("IsRemote(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// TestStorageURLIsBucket covers IsBucket for bucket-only, key, prefix, and
// local URLs.
func TestStorageURLIsBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		want     bool
		wantErr  bool
	}{
		{"s3://bucket", true, false},
		{"s3://bucket/file", false, false},
		{"s3://bucket/prefix/", false, false},
		{"bucket", false, false},
		{"s3://", false, true},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("expected error for %q", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for %q: %v", tc.input, err)
			continue
		}
		if got := u.IsBucket(); got != tc.want {
			t.Errorf("IsBucket(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// TestStorageURLIsPrefix verifies IsPrefix returns true only for remote URLs
// whose path ends with "/".
func TestStorageURLIsPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"s3://bucket/prefix/", true},
		{"s3://bucket/key", false},
		{"s3://bucket", false},
		{"local/dir/", false},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.IsPrefix(); got != tc.want {
			t.Errorf("IsPrefix(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// TestStorageURLIsWildcard checks IsWildcard for paths with and without glob
// characters, and confirms that raw mode disables wildcard detection.
func TestStorageURLIsWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  bool
	}{
		{"s3://bucket/*.log", true},
		{"s3://bucket/logs/?/c", true},
		{"s3://bucket/logs/file", false},
		{"s3://bucket", false},
		{"file*.txt", true},
		{"plain", false},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.IsWildcard(); got != tc.want {
			t.Errorf("IsWildcard(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}

	// raw mode disables wildcard detection.
	u, err := NewStorageURL("s3://bucket/*.log", WithRaw(true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.IsWildcard() {
		t.Errorf("IsWildcard() with raw mode = true, want false")
	}
	if u.IsRaw() != true {
		t.Errorf("IsRaw() = false, want true")
	}
}

// TestSetPrefixAndFilter verifies that setPrefixAndFilter splits a wildcard
// path into prefix + filter, and that a plain path becomes its own prefix
// with the "/" delimiter.
func TestSetPrefixAndFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantPrefix   string
		wantFilter   string
		wantRegex    string
		wantDelim    string
	}{
		{
			name:       "wildcard_log",
			path:       "logs/*.log",
			wantPrefix: "logs/",
			wantFilter: "*.log",
			wantRegex:  strutil.AddNewLineFlag(strutil.MatchFromStartToEnd(regexp.QuoteMeta("logs/") + strutil.WildCardToRegexp("*.log"))),
			wantDelim:  "",
		},
		{
			name:       "wildcard_nested",
			path:       "a/b_c/*/de/*/test",
			wantPrefix: "a/b_c/",
			wantFilter: "*/de/*/test",
			wantRegex:  strutil.AddNewLineFlag(strutil.MatchFromStartToEnd(regexp.QuoteMeta("a/b_c/") + strutil.WildCardToRegexp("*/de/*/test"))),
			wantDelim:  "",
		},
		{
			name:       "plain_key",
			path:       "a/b_c/d/e",
			wantPrefix: "a/b_c/d/e",
			wantFilter: "",
			wantRegex:  strutil.AddNewLineFlag(strutil.MatchFromStartToEnd(regexp.QuoteMeta("a/b_c/d/e") + ".*")),
			wantDelim:  "/",
		},
		{
			name:       "prefix_with_trailing_slash",
			path:       "a/b_c/",
			wantPrefix: "a/b_c/",
			wantFilter: "",
			wantRegex:  strutil.AddNewLineFlag(strutil.MatchFromStartToEnd(regexp.QuoteMeta("a/b_c/") + ".*")),
			wantDelim:  "/",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u := &StorageURL{Path: tc.path}
			if err := u.setPrefixAndFilter(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if u.Prefix != tc.wantPrefix {
				t.Errorf("Prefix = %q, want %q", u.Prefix, tc.wantPrefix)
			}
			if u.filter != tc.wantFilter {
				t.Errorf("filter = %q, want %q", u.filter, tc.wantFilter)
			}
			if u.Delimiter != tc.wantDelim {
				t.Errorf("Delimiter = %q, want %q", u.Delimiter, tc.wantDelim)
			}
			if u.filterRegex == nil {
				t.Fatalf("filterRegex is nil")
			}
			if got := u.filterRegex.String(); got != tc.wantRegex {
				t.Errorf("filterRegex = %q, want %q", got, tc.wantRegex)
			}
		})
	}
}

// TestStorageURLMatch covers wildcard matching semantics: *.log matches
// a.log but not a.txt; ? matches exactly one character; * does not cross /
// for wildcard patterns whose prefix already pins the directory.
//
// Note: the filter regex produced by setPrefixAndFilter uses ".*" for
// "*", which CAN cross "/" — so for s3://bucket/*.log a key like
// "a/b.log" matches. The "does not cross /" guarantee applies to wildcard
// patterns whose prefix is a directory (e.g. s3://bucket/logs/*.log —
// there, "logs/sub/a.log" still matches because .* is greedy, but the
// relative-path parser parseBatch only trims back to the last "/" before
// the filter). These cases pin down the actual behaviour of the layer
// rather than the shell-glob intuition.
func TestStorageURLMatch(t *testing.T) {
	t.Parallel()

	type want struct {
		matched bool
		rel     string
	}
	tests := []struct {
		name string
		url  string
		keys map[string]want
	}{
		{
			name: "log_wildcard",
			url:  "s3://bucket/*.log",
			keys: map[string]want{
				"a.log":   {true, "a.log"},
				"a.txt":   {},
				"a/b.log": {true, "a/b.log"}, // * crosses /, relative keeps full key
			},
		},
		{
			name: "question_mark_single_char",
			url:  "s3://bucket/?at",
			keys: map[string]want{
				"cat":  {true, "cat"},
				"bat":  {true, "bat"},
				"at":   {}, // ? is exactly one char
				"boat": {}, // ? is exactly one char
			},
		},
		{
			name: "wildcard_in_dir",
			url:  "s3://bucket/logs/*.log",
			keys: map[string]want{
				"logs/a.log":     {true, "a.log"},
				"logs/b.log":     {true, "b.log"},
				"logs/c.txt":     {},
				"logs/sub/a.log": {true, "sub/a.log"}, // * crosses /
			},
		},
		{
			name: "non_wildcard_single_key",
			url:  "s3://bucket/key",
			keys: map[string]want{
				"key": {true, "key"},
			},
		},
		{
			name: "non_wildcard_prefix",
			url:  "s3://bucket/key/",
			keys: map[string]want{
				"key/a/":           {true, "a/"},
				"key/test.txt":     {true, "test.txt"},
				"key/test.pdf":     {true, "test.pdf"},
				"key/test.pdf/aaa": {true, "test.pdf/"},
			},
		},
		{
			name: "no_match_invalid_prefix",
			url:  "s3://bucket/key",
			keys: map[string]want{
				"anotherkey":       {},
				"invalidkey/dummy": {},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u, err := NewStorageURL(tc.url)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for key, want := range tc.keys {
				// Reset relativePath so prior matches do not leak.
				u.relativePath = ""
				got := u.Match(key)
				if got != want.matched {
					t.Errorf("Match(%q) = %v, want %v", key, got, want.matched)
				}
				if got && want.rel != "" && u.Relative() != want.rel {
					t.Errorf("Relative() after Match(%q) = %q, want %q", key, u.Relative(), want.rel)
				}
			}
		})
	}
}

// TestStorageURLBase covers Base for both remote and local URLs.
func TestStorageURLBase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"s3://bucket/key.txt", "key.txt"},
		{"s3://bucket/a/b/c", "c"},
		// path.Base("") == "."; this is the standard library behaviour.
		{"s3://bucket", "."},
		{"/local/path/file.txt", "file.txt"},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.Base(); got != tc.want {
			t.Errorf("Base(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestStorageURLDir covers Dir for both remote and local URLs.
func TestStorageURLDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"s3://bucket/a/b/c", "a/b"},
		{"s3://bucket/key", "."},
		{"/local/a/b/c", "/local/a/b"},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.Dir(); got != tc.want {
			t.Errorf("Dir(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestStorageURLJoin ensures remote Join preserves adjacent slashes while
// local Join cleans them via path.Join.
func TestStorageURLJoin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		before *StorageURL
		arg    string
		after  *StorageURL
	}{
		{
			name:   "remote_preserves_adjacent_slashes",
			before: &StorageURL{Type: remoteObject, Scheme: "s3", Bucket: "bucket", Path: "a//b/"},
			arg:    "test.txt",
			after:  &StorageURL{Type: remoteObject, Scheme: "s3", Bucket: "bucket", Path: "a//b/test.txt"},
		},
		{
			name:   "remote_arg_with_slashes",
			before: &StorageURL{Type: remoteObject, Scheme: "s3", Bucket: "bucket", Path: "a/b/"},
			arg:    "folder//test.txt",
			after:  &StorageURL{Type: remoteObject, Scheme: "s3", Bucket: "bucket", Path: "a/b/folder//test.txt"},
		},
		{
			name:   "local_cleans_adjacent_slashes",
			before: &StorageURL{Type: localObject, Path: "dir/a//b/"},
			arg:    "test.txt",
			after:  &StorageURL{Type: localObject, Path: "dir/a/b/test.txt"},
		},
		{
			name:   "local_arg_with_slashes",
			before: &StorageURL{Type: localObject, Path: "dir/a/b/"},
			arg:    "folder//test.txt",
			after:  &StorageURL{Type: localObject, Path: "dir/a/b/folder/test.txt"},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := tc.before.Join(tc.arg)
			if !reflect.DeepEqual(got, tc.after) {
				t.Errorf("Join() got = %+v, want %+v", got, tc.after)
			}
		})
	}
}

// TestStorageURLClone verifies that Clone produces an equal but independent
// copy, and that mutating the clone does not affect the original.
func TestStorageURLClone(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	clone := u.Clone()
	if !reflect.DeepEqual(u, clone) {
		t.Errorf("Clone() not equal to original: got %+v, want %+v", clone, u)
	}
	clone.Path = "modified"
	if u.Path == "modified" {
		t.Errorf("Clone() did not copy: original mutated")
	}
}

// TestStorageURLRelativeAndSetRelative checks Relative falls back to the
// absolute URL when no relative path has been set, and that SetRelative
// computes the relative path against a base.
func TestStorageURLRelativeAndSetRelative(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/parent/child/object2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := u.Relative(); got != u.Absolute() {
		t.Errorf("Relative() = %q, want fallback %q", got, u.Absolute())
	}

	base, err := NewStorageURL("s3://bucket/parent/child/object")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	u.SetRelative(base)
	if want := "object2"; u.Relative() != want {
		t.Errorf("Relative() after SetRelative = %q, want %q", u.Relative(), want)
	}
}

// TestStorageURLSetRelativeWildcardBase verifies the wildcard branch of
// SetRelative: the base path is truncated at the first glob character
// before the relative path is computed.
func TestStorageURLSetRelativeWildcardBase(t *testing.T) {
	t.Parallel()

	sep := string(filepath.Separator)

	base, err := NewStorageURL("/parent/child/object")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	target, err := NewStorageURL("/parent/child2/object")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	target.SetRelative(base)
	want := strings.Join([]string{"..", "child2", "object"}, sep)
	if target.Relative() != want {
		t.Errorf("SetRelative non-wildcard base: got %q, want %q", target.Relative(), want)
	}

	// Wildcard base: the "*" is stripped to "/parent/" before computing
	// the relative path, so the target "/parent/child/object" is relative
	// to "/parent/" => "child/object".
	wbase, err := NewStorageURL("/parent/*/object")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wtarget, err := NewStorageURL("/parent/child/object")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wtarget.SetRelative(wbase)
	wwant := strings.Join([]string{"child", "object"}, sep)
	if wtarget.Relative() != wwant {
		t.Errorf("SetRelative wildcard base: got %q, want %q", wtarget.Relative(), wwant)
	}
}

// TestStorageURLAbsolute verifies the String/Absolute form for remote and
// local URLs.
func TestStorageURLAbsolute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"s3://bucket/key", "s3://bucket/key"},
		{"s3://bucket", "s3://bucket"},
		{"/local/path", "/local/path"},
	}
	for _, tc := range tests {
		u, err := NewStorageURL(tc.input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.input, err)
		}
		if got := u.Absolute(); got != tc.want {
			t.Errorf("Absolute(%q) = %q, want %q", tc.input, got, tc.want)
		}
		if got := u.String(); got != tc.want {
			t.Errorf("String(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestParseBatch covers the batch (wildcard) relative-path parser.
func TestParseBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"do_nothing_if_key_does_not_include_prefix", "a/b/c", "d/e", "d/e"},
		{"do_nothing_if_prefix_does_not_include_slash", "some_random_string", "a/b", "a/b"},
		{"parse_key_if_prefix_is_a_dir", "a/b/", "a/b/c/d", "c/d"},
		{"parse_key_if_prefix_is_not_a_dir", "a/b", "a/b/asset.txt", "b/asset.txt"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := parseBatch(tc.prefix, tc.key); got != tc.want {
				t.Errorf("parseBatch() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestParseNonBatch covers the non-batch (prefix) relative-path parser.
func TestParseNonBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"do_nothing_if_key_does_not_include_prefix", "a/b/c", "d/e", "d/e"},
		{"do_nothing_if_prefix_equals_to_key", "a/b", "a/b", "a/b"},
		{"parse_key_and_return_first_dir_after_prefix", "a/b/", "a/b/c/d", "c/"},
		{"parse_key_and_return_asset_after_prefix", "a/b", "a/b/asset.txt", "asset.txt"},
		{"parse_key_and_return_current_asset_if_prefix_is_not_dir", "a/b/ab", "a/b/abc.txt", "abc.txt"},
		{"parse_key_and_return_current_dir_if_prefix_is_not_dir", "test", "testdir/", "testdir/"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := parseNonBatch(tc.prefix, tc.key); got != tc.want {
				t.Errorf("parseNonBatch() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestHasGlobCharacter checks the wildcard detector used by the parser.
func TestHasGlobCharacter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		s    string
		want bool
	}{
		{"s3://a*/b", true},
		{"s3://a/?/c", true},
		{"s3://a/b/c", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := hasGlobCharacter(tc.s); got != tc.want {
			t.Errorf("hasGlobCharacter(%q) = %v, want %v", tc.s, got, tc.want)
		}
	}
}

// TestStorageURLEscapedPath verifies that EscapedPath percent-encodes each
// path element but keeps the slashes intact. url.QueryEscape encodes a
// space as "+", which is the documented behaviour, so we expect "+" here.
func TestStorageURLEscapedPath(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/a b/c d.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "bucket/a+b/c+d.txt"
	if got := u.EscapedPath(); got != want {
		t.Errorf("EscapedPath() = %q, want %q", got, want)
	}
}

// TestStorageURLMarshalJSON verifies the JSON form is the quoted absolute URL.
func TestStorageURLMarshalJSON(t *testing.T) {
	t.Parallel()

	u, err := NewStorageURL("s3://bucket/key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := u.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error: %v", err)
	}
	want := `"s3://bucket/key"`
	if string(got) != want {
		t.Errorf("MarshalJSON() = %s, want %s", got, want)
	}
}

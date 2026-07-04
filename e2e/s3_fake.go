package e2e

import (
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/igungor/gofakes3"
	"github.com/igungor/gofakes3/backend/s3mem"
)

// s3ServerEndpoint starts an in-process gofakes3 server backed by the
// in-memory backend, and returns its base URL. The server is closed
// automatically when the test ends.
//
// We use the in-memory backend rather than the bolt backend so the e2e
// tests do not touch the filesystem and can run in parallel without
// contending on a boltdb lock.
func s3ServerEndpoint(t *testing.T) string {
	t.Helper()
	backend := s3mem.New()
	faker := gofakes3.New(
		backend,
		gofakes3.WithLogger(gofakes3.GlobalLog(gofakes3.LogLevel("err"))),
	)
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)
	return srv.URL
}

// s3BucketFromTestName returns a DNS-safe bucket name derived from the
// test name. It lower-cases the name and replaces any non-{a-z0-9-}
// character with a dash, then trims to 50 chars and adds a 7-char random
// suffix.
func s3BucketFromTestName(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	out := make([]byte, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, byte(r))
		default:
			out = append(out, '-')
		}
	}
	// Trim leading/trailing dashes and collapse runs.
	s := strings.Trim(string(out), "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 42 {
		s = s[:42]
	}
	return s + "-" + randomString(7)
}

// randomString returns n lowercase-alphanumeric characters.
//
// We use math/rand/v2's top-level functions, which are auto-seeded by the
// Go runtime, to avoid the byte-index modulo bias the previous
// implementation had (a zero-byte b[i] always mapped to 'a', skewing the
// distribution). The bucket name only needs to be unique within the test
// process.
func randomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.IntN(len(alphabet))]
	}
	return string(b)
}

// ensureEnvOrDefault returns the env var value or the default if unset.
func ensureEnvOrDefault(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}

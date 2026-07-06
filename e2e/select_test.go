package e2e

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/igungor/gofakes3"
	"github.com/igungor/gofakes3/backend/s3mem"
)

// TestE2E_SelectNestedSubcommandEndpoint is the e2e regression test for the
// P0 where the shared persistent flags were read from cmd.Parent() instead
// of cmd.Root(): for the nested `select csv/json/...` subcommands the parent
// is `select` (whose own PersistentFlags set is empty), so --endpoint-url
// and friends were silently dropped and requests went to real AWS.
//
// gofakes3 does not implement SelectObjectContent, so the test wraps the
// fake server with a handler that counts ?select requests and answers them
// itself. The assertion is on the wire: the nested subcommand's requests
// (HeadObject + SelectObjectContent) MUST arrive at the local endpoint
// passed via --endpoint-url.
func TestE2E_SelectNestedSubcommandEndpoint(t *testing.T) {
	t.Parallel()

	backend := s3mem.New()
	faker := gofakes3.New(
		backend,
		gofakes3.WithLogger(gofakes3.GlobalLog(gofakes3.LogLevel("err"))),
	)
	inner := faker.Server()

	var selectRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("select") {
			// SelectObjectContent reached the local endpoint; gofakes3
			// cannot serve it, so answer with an S3-style error ourselves.
			selectRequests.Add(1)
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	endpoint := srv.URL

	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)
	putObject(t, client, bucket, "data.json", `{"a":1}`)

	workdir := t.TempDir()
	res := runS6cmd(t, workdir, endpoint, "select", "json",
		"-e", "SELECT * FROM S3Object s", "s3://"+bucket+"/data.json")

	// The query itself fails (the fake answers 501), but that is fine: the
	// point of the test is that the nested subcommand honoured
	// --endpoint-url at all. Before the Root() fix, no request ever reached
	// the local server (HeadObject already went elsewhere) so the counter
	// stayed at zero.
	if got := selectRequests.Load(); got == 0 {
		t.Fatalf("select subcommand never contacted the --endpoint-url server; "+
			"the shared persistent flags were dropped\nstdout: %s\nstderr: %s",
			res.Stdout, res.Stderr)
	}
	if res.ExitCode == 0 {
		t.Errorf("expected a non-zero exit (fake server answers 501 to select), got 0\nstdout: %s", res.Stdout)
	}
}

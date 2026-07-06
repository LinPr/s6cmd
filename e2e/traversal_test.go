package e2e

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestE2E_DownloadRejectsTraversalKey exercises the path-traversal guard
// end-to-end: a bucket contains an object whose key climbs out of the
// destination directory ("../../evil.txt" — gofakes3 stores such keys
// verbatim). A recursive download must fail that object with the traversal
// error and must not write anything outside the destination directory.
// EnsureLocalRelPath is unit-tested in the storage package; this test pins
// that cp's prepare path actually consults it for a malicious remote key.
func TestE2E_DownloadRejectsTraversalKey(t *testing.T) {
	t.Parallel()
	endpoint := s3ServerEndpoint(t)
	client := s3Client(t, endpoint)
	bucket := s3BucketFromTestName(t)
	createBucket(t, client, bucket)

	putObject(t, client, bucket, "safe.txt", "safe")
	putObject(t, client, bucket, "../../evil.txt", "evil")

	// outer contains everything the test touches; dest is nested two levels
	// deep so the "../../evil.txt" key would land inside outer (and be
	// detectable) if the guard ever regressed.
	outer := t.TempDir()
	dest := filepath.Join(outer, "a", "b", "dest")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", dest, err)
	}

	res := runS6cmd(t, outer, endpoint, "cp", "s3://"+bucket+"/*", dest+string(os.PathSeparator))
	if res.ExitCode == 0 {
		t.Fatalf("cp with a traversal key must fail, got exit 0\nstdout: %s\nstderr: %s", res.Stdout, res.Stderr)
	}
	combined := res.Stdout + res.Stderr
	if !strings.Contains(combined, "outside the destination") {
		t.Errorf("expected the traversal error to be reported, got\nstdout: %s\nstderr: %s", res.Stdout, res.Stderr)
	}

	// Nothing named evil.txt may exist anywhere under outer — neither at
	// the escape target (outer/a/evil.txt) nor inside dest.
	err := filepath.WalkDir(outer, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "evil.txt" {
			t.Errorf("traversal key was written to %q", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

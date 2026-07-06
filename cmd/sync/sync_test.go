package sync

import (
	"testing"

	"github.com/LinPr/s6cmd/storage"
)

// mustURL builds a StorageURL from s, optionally overriding the relative
// path the way sync's listObjects does for listed objects.
func mustURL(t *testing.T, s, rel string) *storage.StorageURL {
	t.Helper()
	u, err := storage.NewStorageURL(s)
	if err != nil {
		t.Fatalf("NewStorageURL(%q): %v", s, err)
	}
	if rel != "" {
		u.SetRelativePath(rel)
	}
	return u
}

// obj wraps mustURL into a minimal storage.Object for plan tests.
func obj(t *testing.T, s, rel string) *storage.Object {
	t.Helper()
	return &storage.Object{StorageURL: mustURL(t, s, rel)}
}

// dstKeys extracts the plan keys of the extra (delete-candidate) objects.
func dstKeys(objects []*storage.Object) []string {
	keys := make([]string, 0, len(objects))
	for _, o := range objects {
		keys = append(keys, syncPlanKey(o.StorageURL))
	}
	return keys
}

// TestBuildSyncPlan_SingleObjectDifferentBasename is the regression test
// for the sync --delete data loss: a single-object sync to a destination
// key with a different basename must resolve to exactly that key, pair it
// with the existing destination object, and produce an EMPTY delete set.
// The old Base()-name merge classified the destination as only-destination
// and deleted the object the copy had just written.
func TestBuildSyncPlan_SingleObjectDifferentBasename(t *testing.T) {
	t.Parallel()
	src := []*storage.Object{obj(t, "newsrc.txt", "")}
	dst := mustURL(t, "s3://bucket/target-root.txt", "")
	dstObjects := []*storage.Object{obj(t, "s3://bucket/target-root.txt", "")}

	items, extras, errs := buildSyncPlan(src, dstObjects, dst, false, false)
	if len(errs) != 0 {
		t.Fatalf("buildSyncPlan errs = %v, want none", errs)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if got := items[0].dstURL.Absolute(); got != "s3://bucket/target-root.txt" {
		t.Errorf("dstURL = %q, want %q", got, "s3://bucket/target-root.txt")
	}
	if items[0].dstObj == nil {
		t.Errorf("dstObj = nil, want existing destination object paired by full key")
	}
	if len(extras) != 0 {
		t.Fatalf("extras = %v, want none: --delete must never enqueue the destination key being written", dstKeys(extras))
	}
}

// TestBuildSyncPlan_SingleObjectSubPrefixDest covers the sub-prefix
// wrinkle: a destination key under a sub-prefix (s3://b/dir/k.txt) must be
// paired with its existing destination object and never end up in the
// delete set.
func TestBuildSyncPlan_SingleObjectSubPrefixDest(t *testing.T) {
	t.Parallel()
	src := []*storage.Object{obj(t, "k-local.txt", "")}
	dst := mustURL(t, "s3://bucket/dir/k.txt", "")
	dstObjects := []*storage.Object{obj(t, "s3://bucket/dir/k.txt", "")}

	items, extras, errs := buildSyncPlan(src, dstObjects, dst, false, false)
	if len(errs) != 0 {
		t.Fatalf("buildSyncPlan errs = %v, want none", errs)
	}
	if len(items) != 1 || items[0].dstObj == nil {
		t.Fatalf("existing destination under sub-prefix not paired: items=%d", len(items))
	}
	if len(extras) != 0 {
		t.Fatalf("extras = %v, want none", dstKeys(extras))
	}
}

// TestBuildSyncPlan_ExtrasByFullPath verifies that the delete set is
// computed with full destination-relative paths: a destination key whose
// Base() matches a source object but whose directory differs is extra, and
// a nested key that exactly matches is not.
func TestBuildSyncPlan_ExtrasByFullPath(t *testing.T) {
	t.Parallel()
	src := []*storage.Object{
		obj(t, "/w/src/x.txt", "x.txt"),
		obj(t, "/w/src/sub/y.txt", "sub/y.txt"),
	}
	dst := mustURL(t, "s3://bucket/pre/", "")
	dstObjects := []*storage.Object{
		obj(t, "s3://bucket/pre/x.txt", "x.txt"),         // written: not extra
		obj(t, "s3://bucket/pre/sub/y.txt", "sub/y.txt"), // written: not extra
		obj(t, "s3://bucket/pre/sub/x.txt", "sub/x.txt"), // Base matches x.txt but path differs: extra
		obj(t, "s3://bucket/pre/stale.txt", "stale.txt"), // extra
	}

	items, extras, errs := buildSyncPlan(src, dstObjects, dst, true, false)
	if len(errs) != 0 {
		t.Fatalf("buildSyncPlan errs = %v, want none", errs)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	for _, item := range items {
		if item.dstObj == nil {
			t.Errorf("source %q not paired with its existing destination", item.srcObj.StorageURL.Relative())
		}
	}
	got := dstKeys(extras)
	want := []string{"s3://bucket/pre/sub/x.txt", "s3://bucket/pre/stale.txt"}
	if len(got) != len(want) {
		t.Fatalf("extras = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("extras[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestBuildSyncPlan_SingleObjectToPrefix verifies that a single-object
// sync into a prefix writes <prefix>/<basename> and that only other keys
// under the prefix are delete candidates.
func TestBuildSyncPlan_SingleObjectToPrefix(t *testing.T) {
	t.Parallel()
	src := []*storage.Object{obj(t, "a.txt", "")}
	dst := mustURL(t, "s3://bucket/pre/", "")
	dstObjects := []*storage.Object{
		obj(t, "s3://bucket/pre/a.txt", "a.txt"),
		obj(t, "s3://bucket/pre/other.txt", "other.txt"),
	}

	items, extras, errs := buildSyncPlan(src, dstObjects, dst, false, false)
	if len(errs) != 0 {
		t.Fatalf("buildSyncPlan errs = %v, want none", errs)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if got := items[0].dstURL.Absolute(); got != "s3://bucket/pre/a.txt" {
		t.Errorf("dstURL = %q, want %q", got, "s3://bucket/pre/a.txt")
	}
	if items[0].dstObj == nil {
		t.Errorf("dstObj = nil, want pre/a.txt paired")
	}
	got := dstKeys(extras)
	if len(got) != 1 || got[0] != "s3://bucket/pre/other.txt" {
		t.Errorf("extras = %v, want [s3://bucket/pre/other.txt]", got)
	}
}

// TestBuildSyncPlan_LocalSingleFileDest verifies the local mirror of the
// single-object fix: syncing one file onto an explicit local file path
// targets that exact path (not <dst>/<basename>) and never marks it extra.
func TestBuildSyncPlan_LocalSingleFileDest(t *testing.T) {
	t.Parallel()
	src := []*storage.Object{obj(t, "/w/newsrc.txt", "")}
	dst := mustURL(t, "/out/target.txt", "")
	dstObjects := []*storage.Object{obj(t, "/out/target.txt", "")}

	items, extras, errs := buildSyncPlan(src, dstObjects, dst, false, false)
	if len(errs) != 0 {
		t.Fatalf("buildSyncPlan errs = %v, want none", errs)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if got := items[0].dstURL.Absolute(); got != "/out/target.txt" {
		t.Errorf("dstURL = %q, want %q", got, "/out/target.txt")
	}
	if items[0].dstObj == nil {
		t.Errorf("dstObj = nil, want existing destination file paired")
	}
	if len(extras) != 0 {
		t.Fatalf("extras = %v, want none", dstKeys(extras))
	}
}

// TestGenerateDestinationURL covers the destination resolution matrix.
func TestGenerateDestinationURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		src      string
		srcRel   string
		dst      string
		isBatch  bool
		dstIsDir bool
		want     string
	}{
		{
			name: "single file to remote key keeps the key",
			src:  "newsrc.txt", dst: "s3://bucket/target-root.txt",
			want: "s3://bucket/target-root.txt",
		},
		{
			name: "single file to remote sub-prefix key keeps the key",
			src:  "newsrc.txt", dst: "s3://bucket/dir/k.txt",
			want: "s3://bucket/dir/k.txt",
		},
		{
			name: "single file to remote prefix joins basename",
			src:  "a.txt", dst: "s3://bucket/pre/",
			want: "s3://bucket/pre/a.txt",
		},
		{
			name: "batch to remote prefix joins relative path",
			src:  "/w/src/sub/y.txt", srcRel: "sub/y.txt", dst: "s3://bucket/pre/",
			isBatch: true,
			want:    "s3://bucket/pre/sub/y.txt",
		},
		{
			name: "single file to local file path keeps the path",
			src:  "s3://bucket/k.txt", dst: "/out/target.txt",
			want: "/out/target.txt",
		},
		{
			name: "single file to local dir joins basename",
			src:  "s3://bucket/k.txt", dst: "/out",
			dstIsDir: true,
			want:     "/out/k.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srcURL := mustURL(t, tt.src, tt.srcRel)
			dstURL := mustURL(t, tt.dst, "")
			got, err := generateDestinationURL(srcURL, dstURL, tt.isBatch, tt.dstIsDir)
			if err != nil {
				t.Fatalf("generateDestinationURL: %v", err)
			}
			if got.Absolute() != tt.want {
				t.Errorf("generateDestinationURL = %q, want %q", got.Absolute(), tt.want)
			}
		})
	}
}

// TestGenerateDestinationURL_TraversalRejected verifies a malicious remote
// key cannot escape a local destination directory.
func TestGenerateDestinationURL_TraversalRejected(t *testing.T) {
	t.Parallel()
	srcURL := mustURL(t, "s3://bucket/pre/x.txt", "../../x.txt")
	dstURL := mustURL(t, "/out", "")
	if _, err := generateDestinationURL(srcURL, dstURL, true, true); err == nil {
		t.Fatalf("generateDestinationURL accepted a traversal relative path")
	}
}

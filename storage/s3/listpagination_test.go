package s3store

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// pagingListServer is a minimal ListObjectsV2 endpoint that always serves
// two pages ("a.txt", then "b.txt") via NextContinuationToken. It records
// the max-keys query parameter of the first request and counts list
// requests, so tests can pin the --page-size and --no-paginate wiring of
// ListObjectsWithPagination. The mockS3 backend cannot be used here: its V2
// handler never paginates.
func pagingListServer(t *testing.T) (*httptest.Server, *struct {
	mu       sync.Mutex
	requests int
	maxKeys  string
}) {
	t.Helper()
	state := &struct {
		mu       sync.Mutex
		requests int
		maxKeys  string
	}{}

	type contents struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	}
	type result struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Name                  string   `xml:"Name"`
		IsTruncated           bool     `xml:"IsTruncated"`
		NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
		Contents              []contents
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		state.requests++
		if state.requests == 1 {
			state.maxKeys = r.URL.Query().Get("max-keys")
		}
		token := r.URL.Query().Get("continuation-token")
		state.mu.Unlock()

		res := result{Name: "paging-bucket"}
		if token == "" {
			res.IsTruncated = true
			res.NextContinuationToken = "page-2"
			res.Contents = []contents{{Key: "a.txt", Size: 1}}
		} else {
			res.Contents = []contents{{Key: "b.txt", Size: 1}}
		}
		w.Header().Set("Content-Type", "application/xml")
		out, _ := xml.Marshal(res)
		_, _ = w.Write(out)
	}))
	t.Cleanup(srv.Close)
	return srv, state
}

// TestListObjectsWithPagination_PageSizeAndNoPaginate verifies that
// pageSize is forwarded as MaxKeys on the wire and that noPaginate stops
// after the first page even when the response says IsTruncated.
func TestListObjectsWithPagination_PageSizeAndNoPaginate(t *testing.T) {
	t.Parallel()

	t.Run("paginates fully by default", func(t *testing.T) {
		t.Parallel()
		srv, state := pagingListServer(t)
		store := newS3Store(t, srv)
		objects, _, err := store.ListObjectsWithPagination(context.Background(), "paging-bucket", "", "", 0, false)
		if err != nil {
			t.Fatalf("ListObjectsWithPagination: %v", err)
		}
		if len(objects) != 2 {
			t.Errorf("objects: want 2 across both pages, got %d", len(objects))
		}
		if state.requests != 2 {
			t.Errorf("requests: want 2, got %d", state.requests)
		}
		if state.maxKeys != "" {
			t.Errorf("max-keys should be absent when pageSize is 0, got %q", state.maxKeys)
		}
	})

	t.Run("pageSize is sent as max-keys", func(t *testing.T) {
		t.Parallel()
		srv, state := pagingListServer(t)
		store := newS3Store(t, srv)
		if _, _, err := store.ListObjectsWithPagination(context.Background(), "paging-bucket", "", "", 500, false); err != nil {
			t.Fatalf("ListObjectsWithPagination: %v", err)
		}
		if state.maxKeys != "500" {
			t.Errorf("max-keys: want %q, got %q", "500", state.maxKeys)
		}
	})

	t.Run("noPaginate stops after the first page", func(t *testing.T) {
		t.Parallel()
		srv, state := pagingListServer(t)
		store := newS3Store(t, srv)
		objects, _, err := store.ListObjectsWithPagination(context.Background(), "paging-bucket", "", "", 0, true)
		if err != nil {
			t.Fatalf("ListObjectsWithPagination: %v", err)
		}
		if len(objects) != 1 {
			t.Errorf("objects: want only the first page (1), got %d", len(objects))
		}
		if state.requests != 1 {
			t.Errorf("requests: want 1, got %d", state.requests)
		}
	})
}

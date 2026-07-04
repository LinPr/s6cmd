package s3store

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestBuildInputSerialization covers the pure mapping from
// storage.SelectQuery fields to the v2 types.InputSerialization. It is
// the only piece of select logic that is pure data and therefore worth a
// focused unit test.
func TestBuildInputSerialization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		query   *storage.SelectQuery
		wantErr bool
		check   func(*testing.T, *types.InputSerialization)
	}{
		{
			name:  "json lines default",
			query: &storage.SelectQuery{InputFormat: "json"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.JSON == nil {
					t.Fatalf("JSON nil")
				}
				if s.JSON.Type != types.JSONTypeLines {
					t.Errorf("Type: want %q, got %q", types.JSONTypeLines, s.JSON.Type)
				}
			},
		},
		{
			name:  "json document",
			query: &storage.SelectQuery{InputFormat: "json", InputContentStructure: "document"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.JSON.Type != types.JSONTypeDocument {
					t.Errorf("Type: want %q, got %q", types.JSONTypeDocument, s.JSON.Type)
				}
			},
		},
		{
			name:  "json gzip",
			query: &storage.SelectQuery{InputFormat: "json", CompressionType: "GZIP"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.CompressionType != types.CompressionTypeGzip {
					t.Errorf("CompressionType: want %q, got %q", types.CompressionTypeGzip, s.CompressionType)
				}
			},
		},
		{
			name:  "csv with header",
			query: &storage.SelectQuery{InputFormat: "csv", FileHeaderInfo: "USE", InputContentStructure: ","},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.CSV == nil {
					t.Fatalf("CSV nil")
				}
				if s.CSV.FileHeaderInfo != types.FileHeaderInfoUse {
					t.Errorf("FileHeaderInfo: want %q, got %q", types.FileHeaderInfoUse, s.CSV.FileHeaderInfo)
				}
				if got := ptrString(s.CSV.FieldDelimiter); got != "," {
					t.Errorf("FieldDelimiter: want %q, got %q", ",", got)
				}
			},
		},
		{
			name:  "parquet",
			query: &storage.SelectQuery{InputFormat: "parquet"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.Parquet == nil {
					t.Fatalf("Parquet nil")
				}
			},
		},
		{
			name:  "csv bzip2",
			query: &storage.SelectQuery{InputFormat: "csv", CompressionType: "BZIP2"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if s.CompressionType != types.CompressionTypeBzip2 {
					t.Errorf("CompressionType: want %q, got %q", types.CompressionTypeBzip2, s.CompressionType)
				}
			},
		},
		{
			name:  "csv default delimiter",
			query: &storage.SelectQuery{InputFormat: "csv", FileHeaderInfo: "NONE"},
			check: func(t *testing.T, s *types.InputSerialization) {
				if got := ptrString(s.CSV.FieldDelimiter); got != "," {
					t.Errorf("FieldDelimiter default: want %q, got %q", ",", got)
				}
				if s.CSV.FileHeaderInfo != types.FileHeaderInfoNone {
					t.Errorf("FileHeaderInfo: want %q, got %q", types.FileHeaderInfoNone, s.CSV.FileHeaderInfo)
				}
			},
		},
		{
			name:    "invalid format",
			query:   &storage.SelectQuery{InputFormat: "xml"},
			wantErr: true,
		},
		{
			name:    "invalid compression",
			query:   &storage.SelectQuery{InputFormat: "json", CompressionType: "ZIP"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildInputSerialization(tc.query)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("buildInputSerialization: %v", err)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

// TestBuildOutputSerialization covers the OutputSerialization mapping.
func TestBuildOutputSerialization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		query   *storage.SelectQuery
		wantErr bool
		check   func(*testing.T, *types.OutputSerialization)
	}{
		{
			name:  "json output",
			query: &storage.SelectQuery{OutputFormat: "json"},
			check: func(t *testing.T, s *types.OutputSerialization) {
				if s.JSON == nil {
					t.Fatalf("JSON nil")
				}
			},
		},
		{
			name:  "csv output with custom delimiter",
			query: &storage.SelectQuery{OutputFormat: "csv", InputContentStructure: "\t"},
			check: func(t *testing.T, s *types.OutputSerialization) {
				if s.CSV == nil {
					t.Fatalf("CSV nil")
				}
				if got := ptrString(s.CSV.FieldDelimiter); got != "\t" {
					t.Errorf("FieldDelimiter: want %q, got %q", "\t", got)
				}
			},
		},
		{
			name:  "default to input format",
			query: &storage.SelectQuery{InputFormat: "json"},
			check: func(t *testing.T, s *types.OutputSerialization) {
				if s.JSON == nil {
					t.Fatalf("JSON nil (default)")
				}
			},
		},
		{
			name:  "default csv delimiter",
			query: &storage.SelectQuery{OutputFormat: "csv"},
			check: func(t *testing.T, s *types.OutputSerialization) {
				if got := ptrString(s.CSV.FieldDelimiter); got != "," {
					t.Errorf("FieldDelimiter default: want %q, got %q", ",", got)
				}
			},
		},
		{
			name:    "invalid output format",
			query:   &storage.SelectQuery{OutputFormat: "xml"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildOutputSerialization(tc.query)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("buildOutputSerialization: %v", err)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

// TestSelect_NilQuery guards against the obvious misuse of passing a nil
// query. It is a cheap test that documents the contract.
func TestSelect_NilQuery(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	defer srv.Close()
	store := newS3Store(t, srv)
	u, err := storage.NewStorageURL("s3://b/k")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	err = store.Select(context.Background(), u, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil query, got nil")
	}
	if !strings.Contains(err.Error(), "query must be non-nil") {
		t.Errorf("error: want substring %q, got %q", "query must be non-nil", err.Error())
	}
}

// TestSelect_EmptyExpression guards against an empty SQL expression.
func TestSelect_EmptyExpression(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	defer srv.Close()
	store := newS3Store(t, srv)
	u, err := storage.NewStorageURL("s3://b/k")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	err = store.Select(context.Background(), u, &storage.SelectQuery{}, nil)
	if err == nil {
		t.Fatal("expected error for empty expression, got nil")
	}
}

// TestSelect_DryRun verifies that a dry-run store short-circuits without
// contacting the backend.
func TestSelect_DryRun(t *testing.T) {
	t.Parallel()
	srv, _ := newMockS3Server(t)
	defer srv.Close()
	store := newS3Store(t, srv, func(o *S3Option) { o.DryRun = true })
	u, err := storage.NewStorageURL("s3://select-bucket/data.json")
	if err != nil {
		t.Fatalf("NewStorageURL: %v", err)
	}
	resultCh := make(chan json.RawMessage, 1)
	close(resultCh)
	err = store.Select(context.Background(), u,
		&storage.SelectQuery{Expression: "SELECT 1", InputFormat: "json"},
		resultCh)
	if err != nil {
		t.Fatalf("dry-run Select: %v", err)
	}
}

// TestParseCompressionType covers the string-to-enum mapping.
func TestParseCompressionType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in      string
		want    types.CompressionType
		wantErr bool
	}{
		{"", types.CompressionTypeNone, false},
		{"NONE", types.CompressionTypeNone, false},
		{"gzip", types.CompressionTypeGzip, false},
		{"BZIP2", types.CompressionTypeBzip2, false},
		{"ZIP", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := parseCompressionType(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCompressionType(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseCompressionType(%q): want %q, got %q", tc.in, tc.want, got)
			}
		})
	}
}

// TestParseFileHeaderInfo covers the string-to-enum mapping.
func TestParseFileHeaderInfo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want types.FileHeaderInfo
	}{
		{"", types.FileHeaderInfoNone},
		{"NONE", types.FileHeaderInfoNone},
		{"use", types.FileHeaderInfoUse},
		{"IGNORE", types.FileHeaderInfoIgnore},
		{"unknown", types.FileHeaderInfoNone},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := parseFileHeaderInfo(tc.in)
			if got != tc.want {
				t.Errorf("parseFileHeaderInfo(%q): want %q, got %q", tc.in, tc.want, got)
			}
		})
	}
}

// ptrString safely dereferences a *string, returning "" for nil. It is
// a tiny helper so the test does not need to import aws.
func ptrString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// silence unused imports if the test file is trimmed.
var _ = errors.New
var _ = http.StatusOK

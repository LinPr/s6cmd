package cliutil

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// Default values for SharedFlags. They mirror s5cmd's cp/sync defaults so
// the out-of-the-box behaviour matches user expectations.
const (
	DefaultCopyConcurrency = 5
	DefaultPartSizeMiB     = 50
)

// MetadataDirectiveCopy and MetadataDirectiveReplace are the two valid
// values for --metadata-directive, matching the S3 CopyObject API.
const (
	MetadataDirectiveCopy    = "COPY"
	MetadataDirectiveReplace = "REPLACE"
)

// megabyte is the conversion factor from MiB to bytes used for --part-size,
// which the user supplies in MiB but storage.Get/Put expects in bytes.
const megabyte = 1024 * 1024

// SharedFlags is the set of flags shared by cp, mv and sync. It mirrors
// s5cmd's NewSharedFlags but is expressed as a plain struct so cobra
// commands can register it via AddToCmd.
//
// All fields are populated by cobra when the corresponding flags are parsed;
// callers should treat the struct as read-only after AddToCmd returns.
type SharedFlags struct {
	// NoFollowSymlinks disables symlink traversal during local walks.
	NoFollowSymlinks bool
	// StorageClass sets the S3 storage class on upload/copy targets.
	StorageClass string
	// Concurrency is the per-object multipart concurrency.
	Concurrency int
	// PartSizeMiB is the per-part size in MiB. Convert to bytes via
	// PartSizeBytes() before passing to storage.Get/Put.
	PartSizeMiB int
	// Metadata is the user-supplied object metadata (key=value, repeatable).
	Metadata map[string]string
	// MetadataDirective controls COPY vs REPLACE semantics on CopyObject.
	MetadataDirective string
	// SSE / SSEKMSKeyID configure server-side encryption on the target.
	SSE string
	SSEKMSKeyID string
	// ACL sets the canned ACL on the target object.
	ACL string
	// CacheControl, Expires, ContentType, ContentEncoding,
	// ContentDisposition map to the corresponding S3 object headers.
	CacheControl       string
	Expires            string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	// ForceGlacierTransfer / IgnoreGlacierWarnings control Glacier handling.
	ForceGlacierTransfer  bool
	IgnoreGlacierWarnings bool
	// SourceRegion / DestinationRegion override the bucket region on
	// either side of a copy/sync.
	SourceRegion      string
	DestinationRegion string
	// Exclude / Include are repeatable wildcard patterns. An object is
	// excluded when it matches any Exclude pattern; if Include patterns
	// are present, only matching objects are included.
	Exclude []string
	Include []string
	// Raw disables wildcard expansion on the source URL.
	Raw bool
}

// NewSharedFlags returns a SharedFlags populated with the default values
// that match s5cmd's cp/sync defaults. cobra will overwrite the fields when
// the user passes explicit flags.
func NewSharedFlags() *SharedFlags {
	return &SharedFlags{
		Concurrency: DefaultCopyConcurrency,
		PartSizeMiB: DefaultPartSizeMiB,
	}
}

// PartSizeBytes returns PartSizeMiB converted to bytes, the unit
// storage.Get/Put expects. It is a method so callers do not have to know
// the conversion factor.
func (sf *SharedFlags) PartSizeBytes() int64 {
	if sf.PartSizeMiB <= 0 {
		return int64(DefaultPartSizeMiB) * megabyte
	}
	return int64(sf.PartSizeMiB) * megabyte
}

// MetadataMap returns a copy of the user metadata map. It returns nil when
// the user did not supply any metadata, so callers can treat a nil result
// as "no metadata".
func (sf *SharedFlags) MetadataMap() map[string]string {
	if len(sf.Metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(sf.Metadata))
	for k, v := range sf.Metadata {
		out[k] = v
	}
	return out
}

// AddToCmd registers the SharedFlags on the given cobra Command. cp/mv/sync
// each call this from their NewXxxCmd so the flag surface stays in sync
// across commands.
func (sf *SharedFlags) AddToCmd(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&sf.NoFollowSymlinks, "no-follow-symlinks", false, "do not follow symbolic links")
	cmd.Flags().StringVar(&sf.StorageClass, "storage-class", "", "set storage class for target (STANDARD, GLACIER, STANDARD_IA, ...)")
	cmd.Flags().IntVar(&sf.Concurrency, "concurrency", DefaultCopyConcurrency, "number of concurrent parts transferred between host and remote server")
	cmd.Flags().IntVar(&sf.PartSizeMiB, "part-size", DefaultPartSizeMiB, "size of each part transferred between host and remote server, in MiB")
	cmd.Flags().StringToStringVar(&sf.Metadata, "metadata", nil, "set arbitrary metadata for the object, e.g. --metadata foo=bar")
	cmd.Flags().StringVar(&sf.MetadataDirective, "metadata-directive", "", "set metadata directive for the object: COPY or REPLACE")
	cmd.Flags().StringVar(&sf.SSE, "sse", "", "perform server-side encryption of the data at its destination, e.g. aws:kms")
	cmd.Flags().StringVar(&sf.SSEKMSKeyID, "sse-kms-key-id", "", "customer master key id for SSE-KMS encryption")
	cmd.Flags().StringVar(&sf.ACL, "acl", "", "set acl for target, e.g. public-read")
	cmd.Flags().StringVar(&sf.CacheControl, "cache-control", "", "set cache control header for object")
	cmd.Flags().StringVar(&sf.Expires, "expires", "", "set expires header for object (RFC3339)")
	cmd.Flags().StringVar(&sf.ContentType, "content-type", "", "set content type header for object")
	cmd.Flags().StringVar(&sf.ContentEncoding, "content-encoding", "", "set content encoding header for object")
	cmd.Flags().StringVar(&sf.ContentDisposition, "content-disposition", "", "set content disposition header for object")
	cmd.Flags().BoolVar(&sf.ForceGlacierTransfer, "force-glacier-transfer", false, "force transfer of glacier objects whether they are restored or not")
	cmd.Flags().BoolVar(&sf.IgnoreGlacierWarnings, "ignore-glacier-warnings", false, "turns off glacier warnings")
	cmd.Flags().StringVar(&sf.SourceRegion, "source-region", "", "set the region of source bucket")
	cmd.Flags().StringVar(&sf.DestinationRegion, "destination-region", "", "set the region of destination bucket")
	cmd.Flags().StringSliceVar(&sf.Exclude, "exclude", nil, "exclude objects with given pattern (repeatable)")
	cmd.Flags().StringSliceVar(&sf.Include, "include", nil, "include objects with given pattern (repeatable)")
	cmd.Flags().BoolVar(&sf.Raw, "raw", false, "disable wildcard operations, useful with filenames that contains glob characters")
}

// ValidateMetadataDirective returns an error when --metadata-directive is
// set to a value other than COPY/REPLACE/"". cp/sync call this in their
// validate step so the user gets a clear error instead of an opaque S3 API
// rejection.
func (sf *SharedFlags) ValidateMetadataDirective() error {
	if sf.MetadataDirective == "" {
		return nil
	}
	switch strings.ToUpper(sf.MetadataDirective) {
	case MetadataDirectiveCopy, MetadataDirectiveReplace:
		return nil
	}
	return fmt.Errorf("metadata-directive must be COPY or REPLACE, got %q", sf.MetadataDirective)
}

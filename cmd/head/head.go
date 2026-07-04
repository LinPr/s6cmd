// Package head implements the `s6cmd head` command. It is the JSON-leaning
// counterpart to `stat`: where stat prints multi-line human-readable output,
// head emits a single JSON object per result so it is easy to consume from
// scripts. The behaviour mirrors s5cmd's head command.
//
// For a bucket target it HeadBuckets and prints the bucket URL; for an
// object target it HeadObjects and prints the object metadata as JSON.
package head

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	"github.com/LinPr/s6cmd/strutil"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// NewHeadCmd creates the `head` command.
func NewHeadCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "head [flags] <source>",
		Short:   "print remote object or bucket metadata as JSON",
		Example: head_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")
	cmd.Flags().BoolVar(&o.Raw, "raw", false, "disable wildcard operations, useful with filenames that contain glob characters")

	return &cmd
}

// Args holds the positional arguments.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the head-specific flags.
type Flags struct {
	VersionID string
	Raw       bool
}

// Options is the closure of Args + Flags + CommonFlags.
type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command, args []string) error {
	o.S3Uri = args[0]
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID), storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}
	if !src.IsRemote() {
		return errors.New("target should be remote object or bucket")
	}
	if src.IsPrefix() {
		return errors.New("target has to be an object or a bucket")
	}
	if src.IsWildcard() && !src.IsRaw() {
		return fmt.Errorf("remote source %q can not contain glob characters", src)
	}
	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}
	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID), storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}

	if src.IsBucket() {
		return o.runBucket(ctx, store, src, out)
	}
	return o.runObject(ctx, store, src, out)
}

func (o *Options) runBucket(ctx context.Context, store *storage.Storage, src *storage.StorageURL, out io.Writer) error {
	bucket, err := store.HeadBucket(ctx, src.Bucket)
	if err != nil {
		return err
	}
	msg := headBucketMessage{Bucket: src.String()}
	if bucket != nil {
		msg.Region = bucket.Region
	}
	fmt.Fprintln(out, msg.JSON())
	return nil
}

func (o *Options) runObject(ctx context.Context, store *storage.Storage, src *storage.StorageURL, out io.Writer) error {
	obj, md, err := store.HeadObject(ctx, src)
	if err != nil {
		return err
	}
	if obj == nil {
		return fmt.Errorf("object not found: %s", src)
	}
	// HeadObject does not surface VersionID on storage.Object; the URL
	// carries it when set by the user, otherwise we leave it empty so
	// the JSON omits the field.
	versionID := src.VersionID
	msg := headObjectMessage{
		Key:                  obj.String(),
		ContentType:          mdContentType(md),
		ServerSideEncryption: mdEncryption(md),
		LastModified:         obj.ModTime,
		ContentLength:        obj.Size,
		StorageClass:         string(obj.StorageClass),
		VersionID:            versionID,
		ETag:                 obj.Etag,
		Metadata:             mdUserDefined(md),
	}
	fmt.Fprintln(out, msg.JSON())
	return nil
}

func mdContentType(md *storage.Metadata) string {
	if md == nil {
		return ""
	}
	return md.ContentType
}

func mdEncryption(md *storage.Metadata) string {
	if md == nil {
		return ""
	}
	return md.EncryptionMethod
}

func mdUserDefined(md *storage.Metadata) map[string]string {
	if md == nil {
		return nil
	}
	return md.UserDefined
}

// headObjectMessage is the JSON payload for a single object HeadObject result.
// It mirrors s5cmd's HeadObjectMessage so consumers can parse either tool's
// output with the same schema.
type headObjectMessage struct {
	Key                  string            `json:"key,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	ServerSideEncryption string            `json:"server_side_encryption,omitempty"`
	LastModified         *time.Time        `json:"last_modified,omitempty"`
	ContentLength        int64             `json:"size,omitempty"`
	StorageClass         string            `json:"storage_class,omitempty"`
	VersionID            string            `json:"version_id,omitempty"`
	ETag                 string            `json:"etag,omitempty"`
	Metadata             map[string]string `json:"metadata"`
}

func (m headObjectMessage) String() string { return m.JSON() }
func (m headObjectMessage) JSON() string   { return strutil.JSON(m) }

// headBucketMessage is the JSON payload for a HeadBucket result.
type headBucketMessage struct {
	Bucket string `json:"bucket"`
	Region string `json:"region,omitempty"`
}

func (m headBucketMessage) String() string { return m.JSON() }
func (m headBucketMessage) JSON() string   { return strutil.JSON(m) }

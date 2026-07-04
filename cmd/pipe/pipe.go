// Package pipe implements the `s6cmd pipe` command. It mirrors s5cmd's
// pipe command: stream os.Stdin to a single remote object via storage.Put.
//
// The destination must be a remote object (not a bucket/prefix/wildcard).
// The stdin adapter exposes only io.Reader — not io.Seeker — so the AWS
// multipart uploader cannot attempt to Seek on stdin (which is not
// seekable) and falls back to its streaming multipart path.
package pipe

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// megabyte is the conversion factor from MiB to bytes used for --part-size,
// which the user supplies in MiB but storage.Put expects in bytes.
const megabyte = 1024 * 1024

// NewPipeCmd creates the `pipe` command.
func NewPipeCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "pipe [flags] <destination>",
		Short:   "stream stdin to a remote object",
		Example: pipe_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.complete(cmd, args); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			ctx := cmd.Context()
			return o.run(ctx)
		},
	}

	cmd.Flags().StringVar(&o.StorageClass, "storage-class", "", "set storage class for target (STANDARD, GLACIER, STANDARD_IA, ...)")
	cmd.Flags().IntVar(&o.Concurrency, "concurrency", cliutil.DefaultCopyConcurrency, "number of concurrent parts transferred between host and remote server")
	cmd.Flags().IntVar(&o.PartSizeMiB, "part-size", cliutil.DefaultPartSizeMiB, "size of each part transferred between host and remote server, in MiB")
	cmd.Flags().StringToStringVar(&o.Metadata, "metadata", nil, "set arbitrary metadata for the object, e.g. --metadata foo=bar")
	cmd.Flags().StringVar(&o.SSE, "sse", "", "perform server-side encryption of the data at its destination, e.g. aws:kms")
	cmd.Flags().StringVar(&o.SSEKMSKeyID, "sse-kms-key-id", "", "customer master key id for SSE-KMS encryption")
	cmd.Flags().StringVar(&o.ACL, "acl", "", "set acl for target, e.g. public-read")
	cmd.Flags().StringVar(&o.CacheControl, "cache-control", "", "set cache control header for object")
	cmd.Flags().StringVar(&o.Expires, "expires", "", "set expires header for object (RFC3339)")
	cmd.Flags().BoolVar(&o.Raw, "raw", false, "disable wildcard operations, useful with filenames that contain glob characters")
	cmd.Flags().StringVar(&o.ContentType, "content-type", "", "set content type header for object")
	cmd.Flags().StringVar(&o.ContentEncoding, "content-encoding", "", "set content encoding header for object")
	cmd.Flags().StringVar(&o.ContentDisposition, "content-disposition", "", "set content disposition header for object")
	cmd.Flags().BoolVarP(&o.NoClobber, "no-clobber", "n", false, "do not overwrite destination if already exists")

	return &cmd
}

// Args holds the positional arguments.
type Args struct {
	DestUri string `validate:"required"`
}

// Flags holds the pipe-specific flags. They mirror s5cmd's
// NewPipeCommandFlags but are expressed as a plain struct so cobra can
// register them directly.
type Flags struct {
	StorageClass       string
	Concurrency        int
	PartSizeMiB        int
	Metadata           map[string]string
	SSE                string
	SSEKMSKeyID        string
	ACL                string
	CacheControl       string
	Expires            string
	Raw                bool
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	NoClobber          bool
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
	o.DestUri = args[0]
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o.Args); err != nil {
		return err
	}
	dst, err := storage.NewStorageURL(o.DestUri, storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}
	if !dst.IsRemote() {
		return errors.New("destination must be a remote object")
	}
	if dst.IsBucket() || dst.IsPrefix() {
		return fmt.Errorf("target %q must be an object", dst)
	}
	if dst.IsWildcard() {
		return fmt.Errorf("target %q can not contain glob characters", o.DestUri)
	}
	return nil
}

func (o *Options) run(ctx context.Context) error {
	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}
	dst, err := storage.NewStorageURL(o.DestUri, storage.WithRaw(o.Raw))
	if err != nil {
		return err
	}

	if err := o.shouldOverride(ctx, store, dst); err != nil {
		if errorpkg.IsWarning(err) {
			log.Debug(log.DebugMessage{Operation: "pipe", Err: err.Error()})
			return nil
		}
		return err
	}

	contentType := o.ContentType
	if contentType == "" {
		contentType = guessContentTypeByExtension(dst.Absolute())
	}

	metadata := storage.Metadata{
		UserDefined:        o.Metadata,
		ACL:                o.ACL,
		CacheControl:       o.CacheControl,
		Expires:            o.Expires,
		StorageClass:       o.StorageClass,
		ContentType:        contentType,
		ContentEncoding:    o.ContentEncoding,
		ContentDisposition: o.ContentDisposition,
		EncryptionMethod:   o.SSE,
		EncryptionKeyID:    o.SSEKMSKeyID,
	}

	partSize := int64(o.PartSizeMiB) * megabyte
	if partSize <= 0 {
		partSize = int64(cliutil.DefaultPartSizeMiB) * megabyte
	}
	concurrency := o.Concurrency
	if concurrency <= 0 {
		concurrency = cliutil.DefaultCopyConcurrency
	}

	// stdinReader wraps os.Stdin so only Read is exposed. See the stdin
	// type comment below for why.
	reader := &stdin{file: os.Stdin}
	if err := store.Put(ctx, reader, dst, metadata, concurrency, partSize); err != nil {
		return err
	}

	log.Info(log.InfoMessage{
		Operation:   "pipe",
		Destination: dst.String(),
	})
	return nil
}

// shouldOverride mirrors s5cmd's Pipe.shouldOverride: when --no-clobber is
// set, stat the destination and return ErrObjectExists if it already exists.
// Without --no-clobber it is a no-op.
func (o *Options) shouldOverride(ctx context.Context, store *storage.Storage, dst *storage.StorageURL) error {
	if !o.NoClobber {
		return nil
	}
	obj, err := store.Stat(ctx, dst)
	if err != nil {
		// Destination does not exist -> no override needed.
		if errors.Is(err, errorpkg.ErrGivenObjectNotFound) {
			return nil
		}
		return err
	}
	if obj == nil {
		return nil
	}
	return errorpkg.ErrObjectExists
}

// guessContentTypeByExtension returns the content type for dst based on its
// extension, falling back to "application/octet-stream" when the extension
// is unknown. It mirrors s5cmd's guessContentTypeByExtension.
func guessContentTypeByExtension(name string) string {
	ct := mime.TypeByExtension(filepath.Ext(name))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// stdin is an io.Reader adapter for *os.File. It deliberately exposes only
// Read — not Seek, ReadAt, or any other method — so the AWS multipart
// uploader's internal sniff logic (which uses a type switch on the reader
// to detect io.ReadSeeker / io.ReaderAt) cannot discover a Seek method on
// os.Stdin. os.Stdin does technically satisfy io.Seeker for some backing
// types (e.g. a regular file redirected from a terminal), but seeking on a
// pipe or a char-device fails at runtime; hiding the method forces the
// uploader down its streaming multipart path, which is the only correct
// behaviour for an unbounded stdin stream.
type stdin struct {
	file *os.File
}

func (s *stdin) Read(p []byte) (int, error) {
	return s.file.Read(p)
}

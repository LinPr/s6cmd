// Package bucketversion implements the `s6cmd bucket-version` command. It
// mirrors s5cmd's bucket-version command: query or set the versioning state
// of an S3 bucket.
//
// Without --set it calls GetBucketVersioning and prints the status; with
// --set Suspended|Enabled it calls SetBucketVersioning. The status string
// is case-insensitive on input and normalized via strutil.CapitalizeFirstRune
// before being sent to S3 so the API accepts "enabled", "ENABLED" and
// "Enabled" alike.
package bucketversion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/log"
	"github.com/LinPr/s6cmd/storage"
	"github.com/LinPr/s6cmd/strutil"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// validStatus is the set of values accepted by --set. Matching is
// case-insensitive (see validate).
const (
	statusEnabled   = "Enabled"
	statusSuspended = "Suspended"
)

// NewBucketVersionCmd creates the `bucket-version` command.
func NewBucketVersionCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "bucket-version [flags] <s3://bucket>",
		Short:   "configure bucket versioning",
		Example: bucketversion_examples,
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

	cmd.Flags().StringVar(&o.Set, "set", "", "set versioning status of bucket: (Suspended, Enabled)")

	return &cmd
}

// Args holds the positional arguments.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the bucket-version-specific flags.
type Flags struct {
	Set string
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
	src, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}
	if !src.IsRemote() || src.Bucket == "" {
		return errors.New("target must be an s3 bucket URL (s3://bucket)")
	}
	if src.Path != "" && !src.IsBucket() {
		return errors.New("target must be a bucket, not an object or prefix")
	}
	if o.Set != "" {
		switch strings.ToLower(o.Set) {
		case strings.ToLower(statusEnabled), strings.ToLower(statusSuspended):
		default:
			return fmt.Errorf("--set must be %q or %q (case-insensitive)", statusEnabled, statusSuspended)
		}
	}
	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}
	src, err := storage.NewStorageURL(o.S3Uri)
	if err != nil {
		return err
	}

	if o.Set != "" {
		status := strutil.CapitalizeFirstRune(o.Set)
		if err := store.SetBucketVersioning(ctx, status, src.Bucket); err != nil {
			return err
		}
		msg := bucketVersionMessage{Bucket: src.Bucket, Status: status, IsSet: true}
		log.Info(msg)
		fmt.Fprintln(out, msg.String())
		return nil
	}

	status, err := store.GetBucketVersioning(ctx, src.Bucket)
	if err != nil {
		return err
	}
	msg := bucketVersionMessage{Bucket: src.Bucket, Status: status, IsSet: false}
	log.Info(msg)
	fmt.Fprintln(out, msg.String())
	return nil
}

// bucketVersionMessage mirrors s5cmd's BucketVersionMessage. The unexported
// isSet field is populated by run() and read by String() so the plain-text
// rendering can distinguish "set to" (after --set) from "is" (query).
type bucketVersionMessage struct {
	Bucket string `json:"bucket"`
	Status string `json:"status"`
	IsSet  bool   `json:"-"`
}

func (v bucketVersionMessage) String() string {
	if v.IsSet {
		return fmt.Sprintf("Bucket versioning for %q is set to %q", v.Bucket, v.Status)
	}
	if v.Status != "" {
		return fmt.Sprintf("Bucket versioning for %q is %q", v.Bucket, v.Status)
	}
	return fmt.Sprintf("%q is an unversioned bucket", v.Bucket)
}

func (v bucketVersionMessage) JSON() string {
	return strutil.JSON(v)
}

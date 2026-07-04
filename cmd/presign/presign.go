// Package presign implements the `s6cmd presign` command. It mirrors s5cmd's
// presign command but uses cobra + the s6cmd storage aggregate.
//
// The command generates a time-limited GET URL for a single remote object
// via storage.Presign and prints it to stdout. Buckets, prefixes and
// wildcards are rejected because there is no single object to sign for.
package presign

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

// defaultExpire is the default validity window for presigned URLs, matching
// s5cmd's default of three hours.
const defaultExpire = 3 * time.Hour

// NewPresignCmd creates the `presign` command.
func NewPresignCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "presign [flags] <source>",
		Short:   "print a presigned remote object URL",
		Example: presign_examples,
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

	cmd.Flags().DurationVar(&o.Expire, "expire", defaultExpire, "URL valid duration")
	cmd.Flags().StringVar(&o.VersionID, "version-id", "", "use the specified version of an object")

	return &cmd
}

// Args holds the positional arguments.
type Args struct {
	S3Uri string `validate:"required"`
}

// Flags holds the presign-specific flags.
type Flags struct {
	Expire    time.Duration
	VersionID string
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
	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID))
	if err != nil {
		return err
	}
	if !src.IsRemote() {
		return errors.New("source must be a remote object")
	}
	if src.IsBucket() || src.IsPrefix() {
		return errors.New("remote source must be an object")
	}
	if src.IsWildcard() {
		return fmt.Errorf("remote source %q can not contain glob characters", src)
	}
	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}
	src, err := storage.NewStorageURL(o.S3Uri, storage.WithVersion(o.VersionID))
	if err != nil {
		return err
	}
	expire := o.Expire
	if expire <= 0 {
		expire = defaultExpire
	}
	url, err := store.Presign(ctx, src, expire)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, url)
	return nil
}

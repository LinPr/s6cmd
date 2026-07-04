// Package version implements the `s6cmd version` command. It mirrors s5cmd's
// version command: a flag-less command that prints the build-time version
// string returned by version.GetHumanVersion().
package version

import (
	"fmt"

	"github.com/LinPr/s6cmd/version"
	"github.com/spf13/cobra"
)

// NewVersionCmd creates the `version` command.
func NewVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "version",
		Short:   "print s6cmd version",
		Example: version_examples,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), version.GetHumanVersion())
			return nil
		},
	}
	return cmd
}

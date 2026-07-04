package tree

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/LinPr/s6cmd/internal/cliutil"
	"github.com/LinPr/s6cmd/storage"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
)

func NewTreeCmd() *cobra.Command {
	o := newOptions()
	cmd := cobra.Command{
		Use:     "tree [flags] <target>",
		Short:   "print a tree view of a local path or s3 prefix",
		Example: tree_examples,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.Target = args[0]
			if err := o.complete(cmd); err != nil {
				return err
			}
			if err := o.validate(); err != nil {
				return err
			}
			return o.run(cmd.Context(), cmd.OutOrStdout())
		},
	}

	return &cmd
}

type Args struct {
	Target string `validate:"required"`
}

type Flags struct{}

type Options struct {
	Args
	Flags
	common cliutil.CommonFlags
}

func newOptions() *Options {
	return &Options{}
}

func (o *Options) complete(cmd *cobra.Command) error {
	o.common = cliutil.LoadParentFlags(cmd)
	return nil
}

func (o *Options) validate() error {
	if err := validator.New().Struct(o); err != nil {
		return err
	}
	return nil
}

func (o *Options) run(ctx context.Context, out io.Writer) error {
	url, err := storage.NewStorageURL(o.Target)
	if err != nil {
		return err
	}

	if !url.IsRemote() {
		return printLocalTree(url.Path, out)
	}

	store, err := cliutil.NewStorage(ctx, o.common)
	if err != nil {
		return err
	}

	keys, err := store.ListS3Keys(ctx, url.Bucket, url.Path)
	if err != nil {
		return err
	}
	return printS3Tree(url.Path, keys, out)
}

type treeNode struct {
	name     string
	children map[string]*treeNode
}

func newTreeNode(name string) *treeNode {
	return &treeNode{name: name, children: map[string]*treeNode{}}
}

func (n *treeNode) add(parts []string) {
	if len(parts) == 0 {
		return
	}
	child, ok := n.children[parts[0]]
	if !ok {
		child = newTreeNode(parts[0])
		n.children[parts[0]] = child
	}
	child.add(parts[1:])
}

func (n *treeNode) print(prefix string, out io.Writer) {
	keys := make([]string, 0, len(n.children))
	for k := range n.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		child := n.children[k]
		connector := "├── "
		nextPrefix := prefix + "│   "
		if i == len(keys)-1 {
			connector = "└── "
			nextPrefix = prefix + "    "
		}
		fmt.Fprintf(out, "%s%s%s\n", prefix, connector, child.name)
		child.print(nextPrefix, out)
	}
}

func printS3Tree(prefix string, keys []string, out io.Writer) error {
	root := newTreeNode(".")
	cleanPrefix := strings.TrimPrefix(prefix, "/")
	for _, key := range keys {
		trimmed := strings.TrimPrefix(key, cleanPrefix)
		trimmed = strings.TrimPrefix(trimmed, "/")
		if trimmed == "" {
			continue
		}
		parts := strings.Split(trimmed, "/")
		root.add(parts)
	}
	root.print("", out)
	return nil
}

func printLocalTree(rootPath string, out io.Writer) error {
	info, err := os.Stat(rootPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		fmt.Fprintln(out, filepath.Base(rootPath))
		return nil
	}

	root := newTreeNode(filepath.Base(rootPath))
	if err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == rootPath {
			return nil
		}
		rel, err := filepath.Rel(rootPath, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		root.add(parts)
		return nil
	}); err != nil {
		return err
	}

	fmt.Fprintln(out, root.name)
	root.print("", out)
	return nil
}

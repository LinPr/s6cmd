package cliutil

import (
	"errors"
	"os"
	"strings"
)

func NormalizeRemotePrefix(prefix string) string {
	if prefix == "" || strings.HasSuffix(prefix, "/") {
		return prefix
	}
	return prefix + "/"
}

func IsLocalDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if strings.HasSuffix(path, string(os.PathSeparator)) || strings.HasSuffix(path, "/") {
				return true, nil
			}
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}


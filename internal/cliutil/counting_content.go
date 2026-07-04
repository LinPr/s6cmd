package cliutil

import (
	"io"
	"mime"
	"net/http"
	"os"
)

func init() {
	// Wire the content-type guesser implementation now that the mime/http
	// imports live here. counting.go references guessContentTypeImpl via a
	// function variable so counting.go itself does not need to import
	// mime/http, keeping its surface minimal for callers that only want
	// the counting reader/writer.
	guessContentTypeImpl = guessContentTypeReal
}

// guessContentTypeReal returns the content type for file based first on
// its extension and, failing that, on a sniff of the first 512 bytes. It
// mirrors s5cmd's guessContentType.
func guessContentTypeReal(file *os.File) string {
	if file == nil {
		return ""
	}
	contentType := mime.TypeByExtension(extensionOf(file.Name()))
	if contentType != "" {
		return contentType
	}
	defer file.Seek(0, io.SeekStart)
	const bufsize = 512
	buf, err := io.ReadAll(io.LimitReader(file, bufsize))
	if err != nil {
		return ""
	}
	return http.DetectContentType(buf)
}

// extensionOf returns the file extension, including the leading dot. It is
// a tiny wrapper around the stdlib so callers do not need to import
// path/filepath just for Ext.
func extensionOf(name string) string {
	for i := len(name) - 1; i >= 0 && name[i] != '/'; i-- {
		if name[i] == '.' {
			return name[i:]
		}
	}
	return ""
}

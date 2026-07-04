package selectcmd

import "strconv"

// strconvUnquoteReal is the real implementation of strconvUnquote. It
// lives in a separate file so select.go does not need to import strconv
// directly, keeping its import block small and focused on the command
// surface.
func strconvUnquoteReal(s string) (string, error) {
	return strconv.Unquote(s)
}

package log

import (
	"fmt"

	"github.com/LinPr/s6cmd/strutil"
)

// Message is the interface that all loggable values implement. String is
// used for plain-text output, JSON for the JSON output mode.
type Message interface {
	fmt.Stringer
	JSON() string
}

// InfoMessage is a generic message for successful operations.
type InfoMessage struct {
	Operation   string  `json:"operation"`
	Success     bool    `json:"success"`
	Source      string  `json:"source,omitempty"`
	Destination string  `json:"destination,omitempty"`
	Object      Message `json:"object,omitempty"`

	// VersionID is exported only so JSON marshalling includes it; it is
	// populated from Source when Destination is empty.
	VersionID string `json:"version_id,omitempty"`
}

// String is the plain-text representation of InfoMessage.
func (i InfoMessage) String() string {
	if i.Source != "" && i.Destination != "" {
		return fmt.Sprintf("%v %v %v", i.Operation, i.Source, i.Destination)
	}
	if i.Destination != "" {
		return fmt.Sprintf("%v %v", i.Operation, i.Destination)
	}
	return fmt.Sprintf("%v %v", i.Operation, i.Source)
}

// JSON is the JSON representation of InfoMessage.
func (i InfoMessage) JSON() string {
	if i.Destination == "" && i.Source != "" {
		i.VersionID = ""
	}
	i.Success = true
	return strutil.JSON(i)
}

// ErrorMessage is a generic message for unsuccessful operations.
type ErrorMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"command,omitempty"`
	Err       string `json:"error"`
}

// String is the plain-text representation of ErrorMessage.
func (e ErrorMessage) String() string {
	if e.Command == "" {
		return e.Err
	}
	return fmt.Sprintf("%q: %v", e.Command, e.Err)
}

// JSON is the JSON representation of ErrorMessage.
func (e ErrorMessage) JSON() string {
	return strutil.JSON(e)
}

// DebugMessage is a generic message for debug-level log entries.
type DebugMessage struct {
	Operation string `json:"operation,omitempty"`
	Command   string `json:"job,omitempty"`
	Err       string `json:"error"`
}

// String is the plain-text representation of DebugMessage.
func (d DebugMessage) String() string {
	if d.Command == "" {
		return d.Err
	}
	return fmt.Sprintf("%q: %v", d.Command, d.Err)
}

// JSON is the JSON representation of DebugMessage.
func (d DebugMessage) JSON() string {
	return strutil.JSON(d)
}

// TraceMessage carries an opaque trace string, typically from the AWS SDK.
type TraceMessage struct {
	Message string `json:"message"`
}

// String is the plain-text representation of TraceMessage.
func (t TraceMessage) String() string {
	return t.Message
}

// JSON is the JSON representation of TraceMessage.
func (t TraceMessage) JSON() string {
	return strutil.JSON(t)
}

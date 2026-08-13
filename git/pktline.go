package git

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Scanner reads pkt-line encoded data from the Git smart HTTP protocol.
//
// Git pkt-line format: each line is prefixed by a 4-byte ASCII hex length
// (including the 4 prefix bytes). Special values:
//   - 0000 = flush packet (end of section)
//   - 0001 = delimiter packet (protocol v2)
//   - 0002 = response-end packet (protocol v2)
type Scanner struct {
	r    io.Reader
	line string
	err  error
	done bool
}

// NewScanner creates a pkt-line scanner over r.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: r}
}

// Scan reads the next line. Returns false when done or on error.
func (s *Scanner) Scan() bool {
	if s.done {
		return false
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(s.r, lenBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			s.done = true
			return false
		}
		s.err = fmt.Errorf("pkt-line read length: %w", err)
		return false
	}

	n, err := strconv.ParseInt(string(lenBuf), 16, 32)
	if err != nil {
		s.err = fmt.Errorf("pkt-line parse length %q: %w", string(lenBuf), err)
		return false
	}

	switch {
	case n == 0: // flush packet
		s.line = ""
		return true
	case n == 1: // delimiter packet (v2)
		s.line = ""
		return true
	case n == 2: // response-end packet (v2)
		s.line = ""
		return true
	case n < 4:
		s.err = fmt.Errorf("invalid pkt-line length %d", n)
		return false
	}

	data := make([]byte, n-4)
	if _, err := io.ReadFull(s.r, data); err != nil {
		s.err = fmt.Errorf("pkt-line read data: %w", err)
		return false
	}

	s.line = strings.TrimSuffix(string(data), "\n")
	return true
}

// Line returns the current line content (without the length prefix and trailing newline).
// Returns "" for flush, delimiter, and response-end packets.
func (s *Scanner) Line() string { return s.line }

// Err returns the first error encountered by the scanner.
func (s *Scanner) Err() error { return s.err }

// uploadPackRequest holds parsed information from a git-upload-pack request body.
type uploadPackRequest struct {
	wants        []string // SHA1/SHA256 hashes the client wants
	isFreshClone bool     // true if no "have" lines (full clone, not incremental fetch)
	isShallow    bool     // true if "shallow" lines present
	isV2         bool     // true if this is a protocol v2 request
	v2Command    string   // v2 command: "ls-refs", "fetch", etc.
	hasFilter    bool     // true if "filter" argument present (partial clone)
}

// parseUploadPackRequest parses a buffered git-upload-pack request body.
// It determines whether the request is a fresh clone (cacheable) or an
// incremental fetch (not cacheable).
//
// Fresh clone: has "want" lines, no "have" lines, no "shallow" lines.
// Incremental fetch: has "have" lines (client already has some objects).
// Shallow clone: has "shallow" lines (truncated history).
func parseUploadPackRequest(body []byte) (uploadPackRequest, error) {
	var req uploadPackRequest
	r := bytes.NewReader(body)
	s := NewScanner(r)

	// Phase 1: collect "want" lines until first flush packet.
	// Phase 2: look for "have" and "shallow" lines until "done" or flush.
	phase := 1
	for s.Scan() {
		line := s.Line()
		switch phase {
		case 1:
			if line == "" { // flush: end of wants
				phase = 2
				continue
			}
			if strings.HasPrefix(line, "want ") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					req.wants = append(req.wants, fields[1])
				}
			}
		case 2:
			if strings.HasPrefix(line, "have ") {
				req.isFreshClone = false
				return req, nil // short-circuit: we know it's incremental
			}
			if strings.HasPrefix(line, "shallow ") {
				req.isShallow = true
			}
			// Deepen directives (deepen, deepen-since, deepen-not, deepen-relative)
			// indicate a truncated history request similar to shallow clones.
			// The resulting pack depends on the deepen parameters and is not
			// deterministically cacheable, so treat as shallow.
			if strings.HasPrefix(line, "deepen") {
				req.isShallow = true
			}
			if line == "done" || line == "" {
				// If we haven't seen any "have" lines and no shallow lines,
				// this is a fresh clone.
				req.isFreshClone = !req.isShallow
				return req, nil
			}
		}
	}
	if err := s.Err(); err != nil {
		return req, err
	}
	// If we reach here without "done", treat as incremental to be safe.
	return req, nil
}

// parseV2UploadPackRequest parses a protocol v2 git-upload-pack POST body.
// Protocol v2 uses a different framing than v1:
//
//	command=<cmd>\n       ← first line declares the command
//	<capability lines>    ← agent, etc.
//	0001                  ← delimiter separating command from arguments
//	<argument lines>      ← want, have, done, filter, shallow, etc.
//	0000                  ← flush
//
// For "ls-refs": always cacheable (returns ref list, equivalent to v1 info/refs).
// For "fetch": same want/have/done logic as v1 — fresh clone if no "have" lines.
func parseV2UploadPackRequest(body []byte) (uploadPackRequest, error) {
	var req uploadPackRequest
	req.isV2 = true
	r := bytes.NewReader(body)
	s := NewScanner(r)

	// Phase 1: read command and capabilities until delimiter (0001) or flush (0000).
	// Phase 2: read arguments (want/have/done/filter/shallow) until flush.
	phase := 1
	for s.Scan() {
		line := s.Line()
		switch phase {
		case 1:
			if line == "" { // flush or delimiter → move to arguments
				phase = 2
				continue
			}
			if after, ok := strings.CutPrefix(line, "command="); ok {
				req.v2Command = after
			}
		case 2:
			if strings.HasPrefix(line, "want ") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					req.wants = append(req.wants, fields[1])
				}
			}
			if strings.HasPrefix(line, "have ") {
				req.isFreshClone = false
				return req, nil // short-circuit: incremental
			}
			if strings.HasPrefix(line, "shallow ") {
				req.isShallow = true
			}
			if strings.HasPrefix(line, "deepen") {
				req.isShallow = true
			}
			if strings.HasPrefix(line, "filter ") {
				req.hasFilter = true
			}
			if line == "done" || line == "" {
				// For "fetch" commands: fresh clone if no have/shallow/filter.
				// For "ls-refs" and other commands: not a clone at all.
				if req.v2Command == "fetch" {
					req.isFreshClone = !req.isShallow && !req.hasFilter
				}
				return req, nil
			}
		}
	}
	if err := s.Err(); err != nil {
		return req, err
	}

	return req, nil
}

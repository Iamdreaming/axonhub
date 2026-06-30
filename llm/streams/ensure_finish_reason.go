// Package streams — generic stream primitives.
// The finish-reason fix lives in llm/pipeline/ensure_finish_reason.go
// to avoid an import cycle (streams itself is imported by llm/httpclient).
package streams

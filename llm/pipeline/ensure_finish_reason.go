package pipeline

import (
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/streams"
)

// ensureToolFinishReason wraps a Stream[*llm.Response] and guarantees that
// when the upstream model returns tool calls in the stream but omits
// finish_reason, a synthetic final chunk with finish_reason="tool_calls"
// is injected.
//
// This fixes providers like GLM that emit tool_call deltas but stop
// streaming immediately without a finish_reason chunk. Agent frameworks
// (Claude Code, Pi Agent, etc.) rely on finish_reason / stop_reason to
// detect the end of a tool-call turn. Without it they report "Stream ended
// without finish_reason" or truncate the response.
func ensureToolFinishReason(stream streams.Stream[*llm.Response]) streams.Stream[*llm.Response] {
	return &toolFinishReasonStream{
		stream: stream,
	}
}

type toolFinishReasonStream struct {
	stream streams.Stream[*llm.Response]

	sawToolCalls bool
	sawFinish    bool
	done         bool

	cachedCurrent *llm.Response // cached from Next() so Current() is idempotent
	injectedItem  *llm.Response
	injectedEmit  bool
}

func (s *toolFinishReasonStream) Next() bool {
	if s.done {
		return s.emitFinishIfNeeded()
	}

	if s.stream.Next() {
		s.cachedCurrent = s.stream.Current()
		s.record(s.cachedCurrent)
		if s.cachedCurrent == llm.DoneResponse {
			s.done = true
		}
		return true
	}

	// Upstream exhausted without DoneResponse.
	s.done = true
	if s.stream.Err() != nil {
		return false
	}
	return s.emitFinishIfNeeded()
}

func (s *toolFinishReasonStream) record(cur *llm.Response) {
	if cur == nil {
		return
	}
	for _, c := range cur.Choices {
		if !s.sawToolCalls && c.Delta != nil && len(c.Delta.ToolCalls) > 0 {
			s.sawToolCalls = true
		}
		if !s.sawFinish && c.FinishReason != nil {
			s.sawFinish = true
		}
	}
}

func (s *toolFinishReasonStream) emitFinishIfNeeded() bool {
	if s.injectedEmit {
		return false
	}
	if !s.sawToolCalls || s.sawFinish {
		return false
	}

	reason := "tool_calls"
	s.injectedItem = &llm.Response{
		Choices: []llm.Choice{{
			Index:        0,
			Delta:        &llm.Message{},
			FinishReason: &reason,
		}},
	}
	s.injectedEmit = true
	return true
}

func (s *toolFinishReasonStream) Current() *llm.Response {
	if s.injectedEmit && s.injectedItem != nil {
		return s.injectedItem
	}
	return s.cachedCurrent
}

func (s *toolFinishReasonStream) Err() error {
	return s.stream.Err()
}

func (s *toolFinishReasonStream) Close() error {
	return s.stream.Close()
}

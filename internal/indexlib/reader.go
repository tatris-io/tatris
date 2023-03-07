// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import (
	"context"
)

type (
	Reader interface {
		OpenReader() error
		Search(ctx context.Context, req QueryRequest, limit, from int) (*QueryResponse, error)
		Count() int
		Close()
	}
	// HookReader wraps another Reader and uses hooks to intercept Reader's funcs.
	// It only supports intercepting the 'Close' func at the present time.
	HookReader struct {
		Reader
		CloseHook func(Reader)
	}
)

func (r *HookReader) Close() {
	if r.CloseHook != nil {
		r.CloseHook(r.Reader)
	} else {
		r.Reader.Close()
	}
}

// UnwrapReader unwraps a reader to the underlying reader if it wraps other reader.
func UnwrapReader(r Reader) Reader {
	if r == nil {
		return nil
	}
	switch x := r.(type) {
	case *HookReader:
		return x.Reader
	default:
		return r
	}
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import (
	"context"
)

type Reader interface {
	OpenReader() error
	Search(ctx context.Context, req QueryRequest, limit int) (*QueryResponse, error)
	Close()
}

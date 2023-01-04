package indexlib

import (
	"context"
)

type Reader interface {
	OpenReader() error
	Search(ctx context.Context, req QueryRequest, limit int) (*QueryResponse, error)
	Close()
}

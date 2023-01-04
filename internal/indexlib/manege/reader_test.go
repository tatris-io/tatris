// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package manege

import (
	"context"
	"github.com/tatris-io/tatris/internal/indexlib"
	"testing"
)

func TestRead(t *testing.T) {
	config := &indexlib.BaseConfig{
		Index: "test",
	}
	reader := GetReader(config)
	if reader == nil {
		t.Log("get reader error!")
		t.FailNow()
	}

	matchQuery := &indexlib.MatchQuery{Match: "tatris", Field: "name"}
	resp, err := reader.Search(context.Background(), matchQuery, 0)
	if err != nil {
		t.Log(err)
	}
	t.Log(resp)

	CloseReader(config)
}

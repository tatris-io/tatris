// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"context"
	"encoding/json"
	"github.com/tatris-io/tatris/internal/indexlib"
	"testing"
)

func TestRead(t *testing.T) {
	config := &indexlib.BaseConfig{
		Index: "storage_product",
	}

	TestWrite(t)

	reader, err := GetReader(config)
	if err != nil {
		t.Log("get reader error!")
		t.FailNow()
	} else {
		matchQuery := &indexlib.MatchQuery{Match: "tatris", Field: "name"}
		resp, err := reader.Search(context.Background(), matchQuery, -1)
		if err != nil {
			t.Log(err)
		}
		marshal, _ := json.Marshal(resp)
		t.Log(string(marshal))

		CloseReader(config)
	}
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package manege

import (
	"github.com/tatris-io/tatris/internal/indexlib"
	"testing"
)

func TestWrite(t *testing.T) {
	config := &indexlib.BaseConfig{
		Index: "test",
	}
	writer := GetWriter(config)
	if writer == nil {
		t.Logf("get writer error!")
		t.FailNow()
	}

	doc := make(map[string]interface{})
	doc["name"] = "tatris"
	doc["describe"] = "Time-aware storage and search system"

	err := writer.Insert("test", doc)
	if err != nil {
		t.Logf("error write index %v", err)
	}
	t.Log("Write success!")

	CloseWriter(config)
}

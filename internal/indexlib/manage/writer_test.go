// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"github.com/tatris-io/tatris/internal/indexlib"
	"testing"
)

func TestWrite(t *testing.T) {
	config := &indexlib.BaseConfig{
		Index: "storage_product",
	}
	if writer, err := GetWriter(config); err != nil {
		t.Logf("get writer error!")
		t.FailNow()
	} else {
		doc := make(map[string]interface{})
		doc["name"] = "tatris"
		doc["desc"] = "Time-aware storage and search system"
		err := writer.Insert("storage_product", doc)
		if err != nil {
			t.Logf("error write index %v", err)
		}
		t.Log("Write success!")

		CloseWriter(config)
	}
}

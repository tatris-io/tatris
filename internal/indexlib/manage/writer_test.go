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
<<<<<<< HEAD
		doc := make(map[string]interface{})
		doc["name"] = "tatris"
		doc["desc"] = "Time-aware storage and search system"
		err := writer.Insert("storage_product", doc)
		if err != nil {
			t.Logf("error write index %v", err)
			t.FailNow()
		}
		t.Log("Write success!")

		writer.Close()
=======
		writeDoc("", writer, t, 10.0)
		writeDoc("_v1", writer, t, 11.11)
		writeDoc("_v2", writer, t, 12.0)
		CloseWriter(config)
>>>>>>> 2666684 (feat: term-level queries (#63))
	}
}

func writeDoc(suffix string, writer indexlib.Writer, t *testing.T, score float64) {
	doc := make(map[string]interface{})
	doc["name"] = "tatris" + suffix
	doc["desc"] = "Time-aware storage and search system"
	doc["score"] = score
	err := writer.Insert("storage_product"+suffix, doc)
	if err != nil {
		t.Logf("error write index %v", err)
	}
	t.Log("Write success!")
}

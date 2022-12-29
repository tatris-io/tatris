// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"github.com/blugelabs/bluge"
	"testing"
)

func TestWrite(t *testing.T) {
	writer, err := GetWriter("fs", "./data", "test")
	if err != nil {
		t.Logf("error get writer %v", err)
		t.FailNow()
	}

	doc := bluge.NewDocument("doc")
	doc.AddField(bluge.NewKeywordField("name", "tatris"))
	doc.AddField(bluge.NewKeywordField("describe", "Time-aware storage and search system"))
	err = writer.Insert(doc)
	if err != nil {
		t.Logf("error write index %v", err)
	}
	t.Log("Write success!")

	CloseWriter(writer)
}

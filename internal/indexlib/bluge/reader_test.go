// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"context"
	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRead(t *testing.T) {
	reader, err := GetReader("fs", "./data", "test")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	req := bluge.NewAllMatches(bluge.NewMatchQuery("tatris").SetField("name"))
	dmi, err := reader.Search(context.Background(), req)
	if err != nil {
		t.Log(err)
	}

	next, err := dmi.Next()
	for err == nil && next != nil {
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				assert.Equal(t, string(value), "doc")
			}
			return true
		})
		if err != nil {
			t.Logf("error accessing stored fields: %v", err)
		}
		next, err = dmi.Next()
	}
	if err != nil {
		t.Logf("error iterating results: %v", err)
	}

	CloseReader(reader)
}

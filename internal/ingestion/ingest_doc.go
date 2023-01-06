// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package ingestion

import (
	"crypto/rand"
	"fmt"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"log"
)

// TODO: make it configurable
var dataPath = "/tmp/tatris/_data"

func IngestDocs(idxName string, docs []map[string]interface{}) error {
	config := &indexlib.BaseConfig{
		Index:    idxName,
		DataPath: dataPath,
	}
	writer, err := manage.GetWriter(config)
	if err != nil {
		return err
	}
	docsWithID := make(map[string]map[string]interface{})
	for _, doc := range docs {
		docID := ""
		if id, ok := doc["_id"]; ok && id != nil && id != "" {
			docID = id.(string)
		} else {
			docID = generateID()
		}
		docsWithID[docID] = doc
	}
	return writer.Batch(docsWithID)
}

// TODO: distributed ID
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

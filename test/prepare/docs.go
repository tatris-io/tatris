// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package prepare is to provide some preparations for running unit tests
package prepare

import (
	"encoding/json"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/ingestion"
	"io"
	"os"
)

func PrepareDocs(index, path string) ([]map[string]interface{}, error) {
	jsonFile, err := os.Open(path)
	if err != nil {
		logger.Errorf("open json file fail: %s", err.Error())
	}
	defer jsonFile.Close()
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		logger.Errorf("read json file fail: %s", err.Error())
		return nil, err
	}
	docs := make([]map[string]interface{}, 0)
	err = json.Unmarshal(jsonData, &docs)
	if err != nil {
		logger.Errorf("parse json fail: %s", err.Error())
		return nil, err
	}
	batchDocs := make([]map[string]interface{}, 0)
	for _, doc := range docs {
		batchDocs = append(batchDocs, doc)
		if len(batchDocs) == 10 {
			err = ingestion.IngestDocs(index, batchDocs)
			if err != nil {
				logger.Errorf("ingest docs fail: %s", err.Error())
				return nil, err
			}
			logger.Infof("ingest docs: %d", len(batchDocs))
			batchDocs = make([]map[string]interface{}, 0)
		}
	}

	if err != nil {
		logger.Errorf("ingest docs fail: ", err.Error())
		return nil, err
	}
	logger.Infof("ingest docs: %d", len(docs))
	return docs, nil
}

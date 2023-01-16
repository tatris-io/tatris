// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package prepare is about some preparations for Tatris to execute unit tests
package prepare

import (
	"encoding/json"
	"fmt"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"io"
	"os"
	"path"
	"runtime"
)

func GetIndex(version string) (*core.Index, error) {
	_, filename, _, _ := runtime.Caller(0)
	indexFilePath := path.Join(path.Dir(path.Dir(filename)), "resources/index.json")
	jsonFile, err := os.Open(indexFilePath)
	if err != nil {
		logger.Errorf("open json file fail: %s", err.Error())
		return nil, err
	}
	defer jsonFile.Close()
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		logger.Errorf("read json file fail: %s", err.Error())
		return nil, err
	}
	index := &core.Index{}
	err = json.Unmarshal(jsonData, &index)
	if err != nil {
		logger.Errorf("parse json fail: %s", err.Error())
		return nil, err
	}
	index.Name = fmt.Sprintf("%s_%s", index.Name, version)
	return index, nil
}

func CreateIndex(version string) (*core.Index, error) {
	index, err := GetIndex(version)
	if err != nil {
		return nil, err
	}
	err = metadata.CreateIndex(index)
	if err != nil {
		logger.Errorf("create index fail: ", err.Error())
		return nil, err
	}
	logger.Infof("create index: %v", index)
	return index, nil
}

func GetDocs() ([]map[string]interface{}, error) {
	_, filename, _, _ := runtime.Caller(0)
	docsFilePath := path.Join(path.Dir(path.Dir(filename)), "resources/docs.json")
	jsonFile, err := os.Open(docsFilePath)
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
	return docs, nil
}

func CreateIndexAndDocs(version string) (*core.Index, []map[string]interface{}, error) {
	index, err := CreateIndex(version)
	if err != nil {
		return nil, nil, err
	}
	docs, err := GetDocs()
	if err != nil {
		return nil, nil, err
	}
	batchDocs := make([]map[string]interface{}, 0)
	for _, doc := range docs {
		batchDocs = append(batchDocs, doc)
		if len(batchDocs) == 10 {
			err = ingestion.IngestDocs(index.Name, batchDocs)
			if err != nil {
				logger.Errorf("ingest docs fail: %s", err.Error())
				return index, nil, err
			}
			logger.Infof("ingest docs: %d", len(batchDocs))
			batchDocs = make([]map[string]interface{}, 0)
		}
	}

	if err != nil {
		logger.Errorf("ingest docs fail: ", err.Error())
		return index, nil, err
	}
	logger.Infof("ingest docs: %d", len(docs))
	return index, docs, nil
}

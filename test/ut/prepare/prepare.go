// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package prepare is about some preparations for Tatris to execute unit tests
package prepare

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/tatris-io/tatris/internal/protocol"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta/metadata"
)

func GetIndex(version string) (*core.Index, error) {
	_, filename, _, _ := runtime.Caller(0)
	indexFilePath := path.Join(path.Dir(path.Dir(filename)), "resources/index.json")
	jsonData, err := os.ReadFile(indexFilePath)
	if err != nil {
		logger.Error("read json file failed", zap.String("msg", err.Error()))
		return nil, err
	}
	index := &core.Index{}
	err = json.Unmarshal(jsonData, &index)
	if err != nil {
		logger.Error("parse json failed", zap.String("msg", err.Error()))
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
		logger.Error("create index failed", zap.String("msg", err.Error()))
		return nil, err
	}
	logger.Info("create index", zap.Any("index", index))
	return index, nil
}

func GetDocs() ([]protocol.Document, error) {
	_, filename, _, _ := runtime.Caller(0)
	docsFilePath := path.Join(path.Dir(path.Dir(filename)), "resources/docs.json")
	jsonData, err := os.ReadFile(docsFilePath)
	if err != nil {
		logger.Error("read json file failed", zap.String("msg", err.Error()))
		return nil, err
	}
	docs := make([]protocol.Document, 0)
	err = json.Unmarshal(jsonData, &docs)
	if err != nil {
		logger.Error("parse json failed", zap.String("msg", err.Error()))
		return nil, err
	}
	return docs, nil
}

func CreateIndexAndDocs(version string) (*core.Index, []protocol.Document, error) {
	index, err := CreateIndex(version)
	if err != nil {
		return nil, nil, err
	}
	docs, err := GetDocs()
	if err != nil {
		return nil, nil, err
	}
	batchDocs := make([]protocol.Document, 0)
	for _, doc := range docs {
		batchDocs = append(batchDocs, doc)
		if len(batchDocs) == 10 {
			err = ingestion.IngestDocs(index, batchDocs)
			if err != nil {
				logger.Error("ingest docs failed", zap.String("msg", err.Error()))
				return index, nil, err
			}
			logger.Info("ingest docs", zap.Int("size", len(batchDocs)))
			batchDocs = make([]protocol.Document, 0)
		}
	}

	if err != nil {
		logger.Error("ingest docs failed ", zap.String("msg", err.Error()))
		return index, nil, err
	}
	// wait wal consume
	time.Sleep(time.Second * 3)
	logger.Info("ingest docs", zap.Int("size", len(docs)))
	return index, docs, nil
}

func GetIndexTemplate(version string) (*protocol.IndexTemplate, error) {
	_, filename, _, _ := runtime.Caller(0)
	templateFilePath := path.Join(path.Dir(path.Dir(filename)), "resources/index_template.json")
	jsonData, err := os.ReadFile(templateFilePath)
	if err != nil {
		logger.Error("read json file failed", zap.String("msg", err.Error()))
		return nil, err
	}
	template := &protocol.IndexTemplate{}
	err = json.Unmarshal(jsonData, &template)
	if err != nil {
		logger.Error("parse json failed", zap.String("msg", err.Error()))
		return nil, err
	}
	template.Name = fmt.Sprintf("%s_%s", template.Name, version)
	return template, nil
}

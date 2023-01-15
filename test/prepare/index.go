// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package prepare

import (
	"encoding/json"
	"fmt"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"io"
	"os"
)

func PrepareIndex(path, version string) (*core.Index, error) {
	jsonFile, err := os.Open(path)
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
	err = metadata.CreateIndex(index)
	if err != nil {
		logger.Errorf("create index fail: ", err.Error())
		return nil, err
	}
	logger.Infof("create index: %v", index)
	return index, nil
}

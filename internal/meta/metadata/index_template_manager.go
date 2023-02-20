// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/patrickmn/go-cache"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/protocol"
)

var templateCache = cache.New(cache.NoExpiration, cache.NoExpiration)

func LoadIndexTemplates() error {
	bytesMap, err := MStore.List(IndexPath)
	if err != nil {
		return err
	}
	for _, bytes := range bytesMap {
		template := &protocol.IndexTemplate{}
		if err := json.Unmarshal(bytes, template); err != nil {
			return err
		}
		templateCache.Set(template.Name, template, cache.NoExpiration)
	}
	return nil
}

func CreateIndexTemplate(template *protocol.IndexTemplate) error {
	logger.Info("create index template", zap.Any("template", template))
	return SaveIndexTemplate(template)
}

func SaveIndexTemplate(template *protocol.IndexTemplate) error {
	json, err := json.Marshal(template)
	if err != nil {
		return err
	}
	indexCache.Set(template.Name, template, cache.NoExpiration)
	return MStore.Set(indexTemplatePrefix(template.Name), json)
}

func GetIndexTemplate(templateName string) (*protocol.IndexTemplate, error) {
	var template *protocol.IndexTemplate
	cachedTemplate, found := indexCache.Get(templateName)
	if found {
		template = cachedTemplate.(*protocol.IndexTemplate)
		return template, nil
	}
	return nil, &errs.IndexTemplateNotFoundError{IndexTemplate: templateName}
}

func DeleteIndexTemplate(templateName string) error {
	indexCache.Delete(templateName)
	return MStore.Delete(indexTemplatePrefix(templateName))
}

func indexTemplatePrefix(name string) string {
	return IndexTemplatePath + name
}

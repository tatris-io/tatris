// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"
	"math"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/errs"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/protocol"
)

var templateCache = cache.New(cache.NoExpiration, cache.NoExpiration)

func LoadIndexTemplates() error {
	bytesMap, err := MStore.List(IndexTemplatePath)
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
	FillTemplateAsDefault(template)
	if err := CheckTemplateValid(template); err != nil {
		return err
	}
	logger.Info("create index template", zap.Any("template", template))
	return SaveIndexTemplate(template)
}

func SaveIndexTemplate(template *protocol.IndexTemplate) error {
	json, err := json.Marshal(template)
	if err != nil {
		return err
	}
	templateCache.Set(template.Name, template, cache.NoExpiration)
	return MStore.Set(indexTemplatePrefix(template.Name), json)
}

func GetIndexTemplate(templateName string) (*protocol.IndexTemplate, error) {
	var template *protocol.IndexTemplate
	cachedTemplate, found := templateCache.Get(templateName)
	if found {
		template = cachedTemplate.(*protocol.IndexTemplate)
		return template, nil
	}
	return nil, &errs.IndexTemplateNotFoundError{IndexTemplate: templateName}
}

func DeleteIndexTemplate(templateName string) error {
	templateCache.Delete(templateName)
	return MStore.Delete(indexTemplatePrefix(templateName))
}

func FillTemplateAsDefault(template *protocol.IndexTemplate) {
	if template.Template.Mappings == nil {
		template.Template.Mappings = &protocol.Mappings{}
	}
	if template.Template.Mappings.Dynamic == "" {
		template.Template.Mappings.Dynamic = consts.DynamicMappingMode
	}
	if template.Template.Settings == nil {
		template.Template.Settings = &protocol.Settings{NumberOfShards: 1, NumberOfReplicas: 1}
	}
}

func CheckTemplateValid(template *protocol.IndexTemplate) error {
	if template.Priority < 0 {
		return &errs.InvalidRangeError{
			Desc:  "index_template.priority",
			Value: template.Priority,
			Left:  0,
			Right: math.MaxInt,
		}
	}
	err := CheckSettings(template.Template.Settings)
	if err != nil {
		return err
	}
	err = CheckMappings(template.Template.Mappings)
	if err != nil {
		return err
	}
	return nil
}

func indexTemplatePrefix(name string) string {
	return IndexTemplatePath + name
}

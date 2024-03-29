// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"
	"math"

	"github.com/minio/pkg/wildcard"
	"github.com/tatris-io/tatris/internal/common/utils"

	cache "github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/errs"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexTemplate(template *protocol.IndexTemplate) error {
	if err := utils.ValidateResourceName(template.Name); err != nil {
		return err
	}
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
	Instance().TemplateCache.Set(template.Name, template, cache.NoExpiration)
	return Instance().MStore.Set(indexTemplatePrefix(template.Name), json)
}

func FindTemplates(indexName string) *protocol.IndexTemplate {
	var template *protocol.IndexTemplate
	for _, item := range Instance().TemplateCache.Items() {
		t := item.Object.(*protocol.IndexTemplate)
		for _, pattern := range t.IndexPatterns {
			if wildcard.Match(pattern, indexName) {
				if template == nil || t.Priority > template.Priority {
					template = t
				}
				break
			}
		}
	}
	return template
}

// ResolveIndexTemplates resolved index templates by comma-separated expressions, each expression
// may be a native template name or a wildcard.
// If you know the complete name of the index template exactly, please use
// GetIndexTemplateExplicitly for better performance.
// errs.IndexTemplateNotFoundError will be returned if there is an expression that does not match
// any index templates.
func ResolveIndexTemplates(exp string) ([]*protocol.IndexTemplate, error) {
	results := make([]*protocol.IndexTemplate, 0)
	// try to resolve by wildcards, including native name matches
	for templateName, item := range Instance().TemplateCache.Items() {
		if utils.WildcardMatch(exp, templateName) {
			results = append(results, item.Object.(*protocol.IndexTemplate))
		}
	}
	if len(results) == 0 {
		return nil, &errs.IndexTemplateNotFoundError{IndexTemplate: exp}
	}
	return results, nil
}

// GetIndexTemplateExplicitly gets the index template precisely by name,
// rather than trying to resolve that by wildcards.
func GetIndexTemplateExplicitly(templateName string) (*protocol.IndexTemplate, error) {
	var template *protocol.IndexTemplate
	cachedTemplate, found := Instance().TemplateCache.Get(templateName)
	if found {
		template = cachedTemplate.(*protocol.IndexTemplate)
		return template, nil
	}
	return nil, &errs.IndexTemplateNotFoundError{IndexTemplate: templateName}
}

func DeleteIndexTemplate(templateName string) error {
	Instance().TemplateCache.Delete(templateName)
	return Instance().MStore.Delete(indexTemplatePrefix(templateName))
}

func FillTemplateAsDefault(template *protocol.IndexTemplate) {
	if template.Template.Mappings == nil {
		template.Template.Mappings = &protocol.Mappings{}
	}
	if template.Template.Mappings.Properties == nil {
		template.Template.Mappings.Properties = make(map[string]*protocol.Property)
	}
	if template.Template.Mappings.Dynamic == "" {
		template.Template.Mappings.Dynamic = consts.DynamicMappingMode
	}
	for _, p := range template.Template.Mappings.Properties {
		if p.Dynamic == "" {
			p.Dynamic = template.Template.Mappings.Dynamic
		}
	}
	if template.Template.Settings == nil {
		template.Template.Settings = &protocol.Settings{
			NumberOfShards:   DefaultNumberOfShards,
			NumberOfReplicas: DefaultNumberOfReplicas,
		}
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

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
)

type BlugeWriter struct {
	*indexlib.BaseConfig
	Writer *bluge.Writer
}

func NewBlugeWriter(config *indexlib.BaseConfig) *BlugeWriter {
	return &BlugeWriter{BaseConfig: config}
}

func (b *BlugeWriter) OpenWriter() error {
	var cfg bluge.Config

	switch b.StorageType {
	case indexlib.FSStorageType:
		cfg = config.GetFSConfig(b.DataPath, b.Index)
	default:
		cfg = config.GetFSConfig(b.DataPath, b.Index)
	}

	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		return err
	}

	b.Writer = writer
	return nil
}

func (b *BlugeWriter) Insert(
	docID string,
	doc map[string]interface{},
	mappings *protocol.Mappings,
) error {
	defer utils.Timerf("bluge insert doc finish, index:%s, ID:%s", b.Index, docID)()
	blugeDoc, err := b.generateBlugeDoc(docID, doc, mappings)
	if err != nil {
		return err
	}
	return b.Writer.Insert(blugeDoc)
}

func (b *BlugeWriter) Batch(
	docs map[string]map[string]interface{},
	mappings *protocol.Mappings,
) error {
	defer utils.Timerf("bluge batch insert %d docs finish, index:%s", len(docs), b.Index)()
	batch := index.NewBatch()
	for docID, doc := range docs {
		blugeDoc, err := b.generateBlugeDoc(docID, doc, mappings)
		if err != nil {
			return err
		}
		batch.Insert(blugeDoc)
	}
	return b.Writer.Batch(batch)
}

func (b *BlugeWriter) Reader() (indexlib.Reader, error) {
	reader, err := b.Writer.Reader()
	if err != nil {
		return nil, err
	}
	return &BlugeReader{b.BaseConfig, reader}, nil
}

func (b *BlugeWriter) Close() {
	if b.Writer != nil {
		err := b.Writer.Close()
		if err != nil {
			log.Printf("fail to close bluge writer for: %s", err)
		}
	}
}

func (b *BlugeWriter) generateBlugeDoc(
	docID string,
	doc map[string]interface{},
	mappings *protocol.Mappings,
) (segment.Document, error) {
	bdoc := bluge.NewDocument(docID)
	for key, value := range doc {
		if value == nil {
			continue
		}

		switch v := value.(type) {
		case []interface{}:
			for _, v := range v {
				err := b.addField(bdoc, key, v, mappings)
				if err != nil {
					return nil, err
				}
			}
		default:
			err := b.addField(bdoc, key, v, mappings)
			if err != nil {
				return nil, err
			}
		}
	}

	source, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	bdoc.AddField(bluge.NewStoredOnlyField(consts.IDField, []byte(docID)))
	bdoc.AddField(bluge.NewStoredOnlyField(consts.IndexField, []byte(b.Index)))
	bdoc.AddField(bluge.NewStoredOnlyField(consts.SourceField, source))
	bdoc.AddField(
		bluge.NewDateTimeField(consts.TimestampField, time.Now()).
			StoreValue().
			Sortable().
			Aggregatable(),
	)

	return bdoc, nil
}

func (b *BlugeWriter) addField(
	bdoc *bluge.Document,
	key string,
	value interface{},
	mappings *protocol.Mappings,
) error {
	// TODO get index mapping, case field type(text、keyword、bool)
	var bfield *bluge.TermField
	switch key {
	case consts.TimestampField:
		bfield = bluge.NewDateTimeField(key, value.(time.Time))
	case consts.IDField:
		bfield = bluge.NewKeywordField(key, value.(string))
	default:
		if p, ok := mappings.Properties[key]; ok {
			field, err := b.addFieldByMappingType(p.Type, key, value)
			if err != nil {
				return err
			}
			bfield = field
		} else {
			bfield = b.addFieldByValueType(key, value)
		}
	}

	bdoc.AddField(bfield)
	return nil
}

func (b *BlugeWriter) addFieldByMappingType(
	mappingType string,
	key string,
	value interface{},
) (*bluge.TermField, error) {
	var bfield *bluge.TermField
	var err error
	if t, ok := consts.MappingTypes[mappingType]; ok {
		switch t {
		case consts.NumericMappingType:
			numericValue, ok := value.(float64)
			if !ok {
				return nil, errors.New("numeric value is not numerical type")
			}
			bfield = bluge.NewNumericField(key, numericValue)
		case consts.KeywordMappingType:
			keywordValue, ok := value.(string)
			if !ok {
				return nil, errors.New("keyword value is not string")
			}
			bfield = bluge.NewKeywordField(key, keywordValue)
		case consts.BoolMappingType:
			boolValue, ok := value.(bool)
			if !ok {
				return nil, errors.New("bool value is not bool")
			}
			bfield = bluge.NewKeywordField(key, strconv.FormatBool(boolValue))
		case consts.TextMappingType:
			textValue, ok := value.(string)
			if !ok {
				return nil, errors.New("text value is not string")
			}
			bfield = bluge.NewTextField(key, textValue)
		case consts.DateMappingType:
			dateValue, ok := value.(string)
			if !ok {
				return nil, errors.New("date value is not string")
			}
			date, err := time.Parse(time.RFC3339, dateValue)
			if err != nil {
				return nil, err
			}
			bfield = bluge.NewDateTimeField(key, date)
		}
	}
	return bfield, err
}

func (b *BlugeWriter) addFieldByValueType(key string, value interface{}) *bluge.TermField {
	var bfield *bluge.TermField
	switch val := value.(type) {
	case string:
		bfield = bluge.NewKeywordField(key, val)
	case float64:
		bfield = bluge.NewNumericField(key, val)
	case bool:
		bfield = bluge.NewKeywordField(key, strconv.FormatBool(val))
	}
	return bfield
}

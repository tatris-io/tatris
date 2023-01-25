// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

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

func (b *BlugeWriter) Insert(docID string, doc map[string]interface{}) error {
	defer utils.Timerf("bluge insert doc finish, index:%s, ID:%s", b.Index, docID)()
	blugeDoc, err := b.generateBlugeDoc(docID, doc)
	if err != nil {
		return nil
	}
	return b.Writer.Insert(blugeDoc)
}

func (b *BlugeWriter) Batch(docs map[string]map[string]interface{}) error {
	defer utils.Timerf("bluge batch insert %d docs finish, index:%s", len(docs), b.Index)()
	batch := index.NewBatch()
	for docID, doc := range docs {
		blugeDoc, err := b.generateBlugeDoc(docID, doc)
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
) (segment.Document, error) {
	bdoc := bluge.NewDocument(docID)
	for key, value := range doc {
		if value == nil {
			continue
		}

		switch v := value.(type) {
		case []interface{}:
			for _, v := range v {
				b.addField(bdoc, key, v)
			}
		default:
			b.addField(bdoc, key, v)
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

func (b *BlugeWriter) addField(bdoc *bluge.Document, key string, value interface{}) {
	// TODO get index mapping, case field type(text、keyword、bool)
	var bfield *bluge.TermField
	switch key {
	case consts.TimestampField:
		bfield = bluge.NewDateTimeField(key, value.(time.Time))
	case consts.IDField:
		bfield = bluge.NewKeywordField(key, value.(string))
	default:
		switch val := value.(type) {
		case string:
			bfield = bluge.NewKeywordField(key, val)
		case float64:
			bfield = bluge.NewNumericField(key, val)
		case bool:
			bfield = bluge.NewKeywordField(key, strconv.FormatBool(val))
		}
	}
	bdoc.AddField(bfield)
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

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
	*indexlib.Config
	Mappings protocol.Mappings
	Index    string
	Segment  string
	Writer   *bluge.Writer
}

func NewBlugeWriter(
	config *indexlib.Config,
	mappings protocol.Mappings,
	index string,
	segment string,
) *BlugeWriter {
	return &BlugeWriter{Config: config, Mappings: mappings, Index: index, Segment: segment}
}

func (b *BlugeWriter) OpenWriter() error {
	var cfg bluge.Config

	switch b.Config.DirectoryType {
	case consts.DirectoryFS:
		cfg = config.GetFSConfig(b.Config.FS.Path, b.Segment)
	case consts.DirectoryOSS:
		cfg = config.GetOSSConfig(
			b.Config.OSS.Endpoint,
			b.Config.OSS.Bucket,
			b.Config.OSS.AccessKeyID,
			b.Config.OSS.SecretAccessKey,
			b.Segment,
			b.Config.OSS.MinimumConcurrencyLoadSize,
		)
	default:
		cfg = config.GetFSConfig(b.Config.FS.Path, b.Segment)
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
	doc protocol.Document,
) error {
	defer utils.Timerf("bluge insert doc finish, segment:%s, ID:%s", b.Segment, docID)()
	blugeDoc, err := b.generateBlugeDoc(docID, doc, b.Mappings)
	if err != nil {
		return err
	}
	return b.Writer.Insert(blugeDoc)
}

func (b *BlugeWriter) Batch(
	docs map[string]protocol.Document,
) error {
	defer utils.Timerf("bluge batch insert %d docs finish, segment:%s", len(docs), b.Segment)()
	batch := index.NewBatch()
	for docID, doc := range docs {
		blugeDoc, err := b.generateBlugeDoc(docID, doc, b.Mappings)
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
	return NewBlugeReader(b.Config, []string{b.Segment}, []*bluge.Reader{reader}, nil), nil
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
	doc protocol.Document,
	mappings protocol.Mappings,
) (segment.Document, error) {
	ts, _ := utils.ParseTime(doc[consts.TimestampField])
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
		bluge.NewDateTimeField(consts.TimestampField, ts).
			Sortable().
			Aggregatable(),
	)

	return bdoc, nil
}

func (b *BlugeWriter) addField(
	bdoc *bluge.Document,
	key string,
	value interface{},
	mappings protocol.Mappings,
) error {
	var bfield *bluge.TermField
	if p, ok := mappings.Properties[key]; ok {
		field, err := b.addFieldByMappingType(p.Type, key, value)
		if err != nil {
			return err
		}
		bfield = field
	}
	if bfield != nil {
		bdoc.AddField(bfield)
	}
	return nil
}

func (b *BlugeWriter) addFieldByMappingType(
	mappingType string,
	key string,
	value interface{},
) (*bluge.TermField, error) {
	var bfield *bluge.TermField
	var err error
	if ok, lType := indexlib.ValidateMappingType(mappingType); ok {
		switch lType.Type {
		case consts.LibFieldTypeNumeric:
			numericValue, ok := value.(float64)
			if !ok {
				return nil, &errs.InvalidFieldValError{Field: key, Type: mappingType, Value: value}
			}
			bfield = bluge.NewNumericField(key, numericValue)
		case consts.LibFieldTypeKeyword:
			keywordValue, ok := value.(string)
			if !ok {
				return nil, &errs.InvalidFieldValError{Field: key, Type: mappingType, Value: value}
			}
			bfield = bluge.NewKeywordField(key, keywordValue)
			bfield.WithAnalyzer(genAnalyzer("keyword"))
		case consts.LibFieldTypeBool:
			boolValue, ok := value.(bool)
			if !ok {
				return nil, &errs.InvalidFieldValError{Field: key, Type: mappingType, Value: value}
			}
			bfield = bluge.NewKeywordField(key, strconv.FormatBool(boolValue))
		case consts.LibFieldTypeText:
			textValue, ok := value.(string)
			if !ok {
				return nil, &errs.InvalidFieldValError{Field: key, Type: mappingType, Value: value}
			}
			bfield = bluge.NewTextField(key, textValue)
			// TODO get analyzer from config
			bfield.WithAnalyzer(genAnalyzer(""))
		case consts.LibFieldTypeDate:
			var date time.Time
			date, err := utils.ParseTime(value)
			if err != nil {
				return nil, &errs.InvalidFieldValError{Field: key, Type: mappingType, Value: value}
			}
			bfield = bluge.NewDateTimeField(key, date)
		}

		if lType.Type != consts.LibFieldTypeText {
			bfield.Sortable().Aggregatable()
		}
	}
	return bfield, err
}

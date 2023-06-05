// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestIndexLib(t *testing.T) {
	// prepare
	index, err := prepare.CreateIndex(
		strings.ReplaceAll(
			time.Now().Format(consts.TimeFmtWithoutSeparator),
			consts.Dot,
			consts.Empty,
		),
	)
	if err != nil {
		t.Fatalf("prepare index fail: %s", err.Error())
	}
	docs, err := prepare.GetDocs()
	if err != nil {
		t.Fatalf("get docs fail: %s", err.Error())
	}

	indexlibCfg := indexlib.BuildConf(config.Cfg.Directory)

	// test
	t.Run("test_write", func(t *testing.T) {
		if writer, err := manage.GetWriter(indexlibCfg, *index.Mappings, index.Name, index.Name); err != nil {
			t.Fatalf("get writer error: %s", err.Error())
		} else {
			defer writer.Close()
			for _, doc := range docs {
				ID := ""
				if docID, ok := doc[consts.IDField]; !ok || docID == "" {
					docID, err := utils.GenerateID()
					if err != nil {
						t.Fatalf("generate docID fail: %s", err.Error())
					}
					doc[consts.IDField] = docID
				}
				err = writer.Insert(ID, doc)
				if err != nil {
					t.Fatalf("write fail: %s", err.Error())
				}
			}
			logger.Info("write success")
		}
	})

	t.Run("test_read", func(t *testing.T) {
		reader, err := manage.GetReader(indexlibCfg, index.Name)
		if err != nil {
			t.Fatalf("get reader error: %s", err.Error())
		}
		defer reader.Close()

		// test match query
		matchQuery := indexlib.NewMatchQuery()
		matchQuery.Match = "Java"
		matchQuery.Field = "lang"
		resp, err := reader.Search(context.Background(), matchQuery, 10, 0)
		assert.NoError(t, err)
		respJSON, err := json.Marshal(resp)
		assert.NoError(t, err)
		logger.Infof("match query result: %s", string(respJSON))

		// test term query
		termQuery := indexlib.NewTermQuery()
		termQuery.Term = "Java"
		termQuery.Field = "lang"
		termResp, err := reader.Search(context.Background(), termQuery, 10, 0)
		assert.NoError(t, err)
		termRespJSON, err := json.Marshal(termResp)
		assert.NoError(t, err)
		logger.Infof("term query result: %s", string(termRespJSON))

		// test terms query
		termsQuery := indexlib.NewTermsQuery()
		termsQuery.Terms = map[string]*indexlib.Terms{
			"name": {
				Fields: []string{"elasticsearch", "meilisearch"},
			},
		}
		termsResp, err := reader.Search(context.Background(), termsQuery, 10, 0)
		assert.NoError(t, err)
		termsRespJSON, err := json.Marshal(termsResp)
		assert.NoError(t, err)
		logger.Infof("terms query result: %s", string(termsRespJSON))

		// test ids query
		idsQuery := indexlib.NewTermsQuery()
		idsQuery.Terms = map[string]*indexlib.Terms{
			"_id": {
				Fields: []string{
					docs[0][consts.IDField].(string),
					docs[1][consts.IDField].(string),
					docs[2][consts.IDField].(string)},
			},
		}
		idsResp, err := reader.Search(context.Background(), idsQuery, 10, 0)
		assert.NoError(t, err)
		idsRespJSON, err := json.Marshal(idsResp)
		assert.NoError(t, err)
		logger.Infof("ids query result: %s", string(idsRespJSON))

		// test range query
		rangeQuery := indexlib.NewRangeQuery()
		rangeQuery.Range = map[string]*indexlib.RangeVal{
			"stars": {
				GTE: 10000,
				LT:  100000,
			},
		}
		rangeResp, err := reader.Search(context.Background(), rangeQuery, 10, 0)
		assert.NoError(t, err)
		rangeRespJSON, err := json.Marshal(rangeResp)
		assert.NoError(t, err)
		logger.Infof("range query result: %s", string(rangeRespJSON))

		// test bool query
		boolQuery := indexlib.NewBooleanQuery()
		boolQuery.Musts = []indexlib.QueryRequest{termQuery}
		boolQuery.Filters = []indexlib.QueryRequest{termsQuery}
		boolResp, err := reader.Search(context.Background(), boolQuery, 10, 0)
		assert.NoError(t, err)
		boolRespJSON, err := json.Marshal(boolResp)
		assert.NoError(t, err)
		logger.Infof("bool query result: %s", string(boolRespJSON))

	})
}

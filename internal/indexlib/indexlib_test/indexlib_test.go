// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/test/prepare"
	"io"
	"os"
	"path"
	"testing"
	"time"
)

const (
	indexPath = "../../../test/materials/index.json"
	docsPath  = "../../../test/materials/docs.json"
)

func TestIndexLib(t *testing.T) {
	// prepare
	start := time.Now()
	version := start.Format(consts.VersionTimeFmt)
	index, err := prepare.PrepareIndex(indexPath, version)
	if err != nil {
		t.Fatalf("prepare index fail: %s", err.Error())
	}

	docs := make([]map[string]interface{}, 0)

	// test
	t.Run("test_write", func(t *testing.T) {
		config := &indexlib.BaseConfig{
			Index: path.Join(consts.DefaultDataPath, index.Name),
		}
		if writer, err := manage.GetWriter(config); err != nil {
			t.Fatalf("get writer error: %s", err.Error())
		} else {
			defer writer.Close()
			// prepare docs
			jsonFile, err := os.Open(docsPath)
			if err != nil {
				t.Fatalf("open json file fail: %s", err.Error())
			}
			defer jsonFile.Close()
			jsonData, err := io.ReadAll(jsonFile)
			if err != nil {
				t.Fatalf("read json file fail: %s", err.Error())
			}
			json.Unmarshal(jsonData, &docs)

			for _, doc := range docs {
				ID := ""
				if docID, ok := doc[consts.IDField]; !ok || docID == "" {
					ID = utils.GenerateID()
					doc[consts.IDField] = ID
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
		config := &indexlib.BaseConfig{
			Index: path.Join(consts.DefaultDataPath, index.Name),
		}

		reader, err := manage.GetReader(config)
		if err != nil {
			t.Fatalf("get reader error: %s", err.Error())
		}
		defer reader.Close()

		// test match query
		matchQuery := &indexlib.MatchQuery{Match: "elasticsearch", Field: "name"}
		resp, err := reader.Search(context.Background(), matchQuery, -1)
		assert.NoError(t, err)
		respJSON, err := json.Marshal(resp)
		assert.NoError(t, err)
		logger.Infof("match query result: %s", string(respJSON))

		// test term query
		termQuery := &indexlib.TermQuery{Term: "elasticsearch", Field: "name"}
		termResp, err := reader.Search(context.Background(), termQuery, 10)
		assert.NoError(t, err)
		termRespJSON, err := json.Marshal(termResp)
		assert.NoError(t, err)
		logger.Infof("term query result: %s", string(termRespJSON))

		// test terms query
		termsQuery := &indexlib.TermsQuery{
			Terms: map[string]*indexlib.Terms{
				"name": {
					Fields: []string{"elasticsearch", "meilisearch"},
				},
			},
		}
		termsResp, err := reader.Search(context.Background(), termsQuery, 10)
		assert.NoError(t, err)
		termsRespJSON, err := json.Marshal(termsResp)
		assert.NoError(t, err)
		logger.Infof("terms query result: %s", string(termsRespJSON))

		// test ids query
		idsQuery := &indexlib.TermsQuery{
			Terms: map[string]*indexlib.Terms{
				"_id": {
					Fields: []string{
						docs[0][consts.IDField].(string),
						docs[1][consts.IDField].(string),
						docs[2][consts.IDField].(string)},
				},
			},
		}
		idsResp, err := reader.Search(context.Background(), idsQuery, 10)
		assert.NoError(t, err)
		idsRespJSON, err := json.Marshal(idsResp)
		assert.NoError(t, err)
		logger.Infof("ids query result: %s", string(idsRespJSON))

		// test range query
		rangeQuery := &indexlib.RangeQuery{Range: map[string]*indexlib.RangeVal{
			"stars": {
				GTE: 10000,
				LT:  100000,
			},
		}}
		rangeResp, err := reader.Search(context.Background(), rangeQuery, 10)
		assert.NoError(t, err)
		rangeRespJSON, err := json.Marshal(rangeResp)
		assert.NoError(t, err)
		logger.Infof("range query result: %s", string(rangeRespJSON))

		// test bool query
		boolQuery := &indexlib.BooleanQuery{
			Musts:   []indexlib.QueryRequest{termQuery},
			Filters: []indexlib.QueryRequest{termsQuery},
		}
		boolResp, err := reader.Search(context.Background(), boolQuery, 10)
		assert.NoError(t, err)
		boolRespJSON, err := json.Marshal(boolResp)
		assert.NoError(t, err)
		logger.Infof("bool query result: %s", string(boolRespJSON))

	})
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"context"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/indexlib"
	"testing"
)

func TestRead(t *testing.T) {
	config := &indexlib.BaseConfig{
		Index: "storage_product",
	}

	TestWrite(t)

	reader, err := GetReader(config)
	if err != nil {
		t.Log("get reader error!")
		t.FailNow()
		return
	}

	matchQuery := &indexlib.MatchQuery{Match: "tatris", Field: "name"}
	resp, matchErr := reader.Search(context.Background(), matchQuery, -1)
	if matchErr != nil {
		t.Log(matchErr)
	}
	respJSON, err := json.Marshal(resp)
	assert.NoError(t, matchErr)
	t.Log("match query result:", string(respJSON))

	// test term query
	termQuery := &indexlib.TermQuery{Term: "tatris_v1", Field: "name"}
	termResp, termErr := reader.Search(context.Background(), termQuery, termQuery.Query().Size)
	if termErr != nil {
		t.Log(termErr)
	}
	termRespJSON, err := json.Marshal(termResp)
	assert.NoError(t, termErr)
	t.Log("term query result:", string(termRespJSON))

	// test terms query
	termsQuery := &indexlib.TermsQuery{
		Terms: map[string]*indexlib.Terms{
			"name": {
				Fields: []string{"tatris_v1", "tatris_v2"},
			},
		},
	}
	termsResp, termsErr := reader.Search(context.Background(), termsQuery, termQuery.Query().Size)
	if termsErr != nil {
		t.Log(termsErr)
	}
	termsRespJSON, err := json.Marshal(termsResp)
	assert.NoError(t, termsErr)
	t.Log("terms query result:", string(termsRespJSON))

	// test ids query
	idsQuery := &indexlib.TermsQuery{
		Terms: map[string]*indexlib.Terms{
			"_id": {
				Fields: []string{"storage_product_v1", "storage_product_v2"},
			},
		},
	}
	idsResp, idsErr := reader.Search(context.Background(), idsQuery, idsQuery.Query().Size)
	if idsErr != nil {
		t.Log(idsErr)
	}
	idsRespJSON, err := json.Marshal(idsResp)
	assert.NoError(t, idsErr)
	t.Log("ids query result:", string(idsRespJSON))

	//test range query
	rangeQuery := &indexlib.RangeQuery{Range: map[string]*indexlib.RangeVal{
		"score": {
			GTE: 11.11,
			LT:  12.0,
		},
	}}
	rangeResp, rangeErr := reader.Search(context.Background(), rangeQuery, rangeQuery.Query().Size)
	if rangeErr != nil {
		t.Log(rangeErr)
	}
	rangeRespJSON, err := json.Marshal(rangeResp)
	assert.NoError(t, rangeErr)
	t.Log("range query result:", string(rangeRespJSON))

	reader.Close()

}

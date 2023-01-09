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
<<<<<<< HEAD
	} else {
		matchQuery := &indexlib.MatchQuery{Match: "tatris", Field: "name"}
		query := &indexlib.BooleanQuery{Musts: []indexlib.QueryRequest{matchQuery}}
		resp, err := reader.Search(context.Background(), query, -1)
		if err != nil {
			t.Log(err)
		}
		marshal, _ := json.Marshal(resp)
		t.Log(string(marshal))

		reader.Close()
=======
		return
	}

	matchQuery := &indexlib.MatchQuery{Match: "tatris", Field: "name"}
	resp, matchErr := reader.Search(context.Background(), matchQuery, -1)
	if matchErr != nil {
		t.Log(matchErr)
	}
	respJson, err := json.Marshal(resp)
	assert.NoError(t, matchErr)
	t.Log("match query result:", string(respJson))

	// test term query
	termQuery := &indexlib.TermQuery{Term: "tatris_v1", Field: "name"}
	termResp, termErr := reader.Search(context.Background(), termQuery, termQuery.Query().Size)
	if termErr != nil {
		t.Log(termErr)
>>>>>>> 2666684 (feat: term-level queries (#63))
	}
	termRespJson, err := json.Marshal(termResp)
	assert.NoError(t, termErr)
	t.Log("term query result:", string(termRespJson))

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
	termsRespJson, err := json.Marshal(termsResp)
	assert.NoError(t, termsErr)
	t.Log("terms query result:", string(termsRespJson))

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
	idsRespJson, err := json.Marshal(idsResp)
	assert.NoError(t, idsErr)
	t.Log("ids query result:", string(idsRespJson))

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
	rangeRespJson, err := json.Marshal(rangeResp)
	assert.NoError(t, rangeErr)
	t.Log("range query result:", string(rangeRespJson))

	CloseReader(config)

}

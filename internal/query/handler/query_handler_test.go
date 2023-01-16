// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	prepare2 "github.com/tatris-io/tatris/internal/ut/prepare"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestQueryHandler(t *testing.T) {

	// prepare
	index, _, err := prepare2.CreateIndexAndDocs(time.Now().Format(consts.VersionTimeFmt))
	if err != nil {
		t.Fatalf("prepare index and docs fail: %s", err.Error())
	}

	// test
	t.Run("test_query_handler", func(t *testing.T) {

		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: index.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewBufferString(queryRequest))
		QueryHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

const queryRequest = `
{
  "size": 20,
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "name": {
              "query": "elasticsearch",
              "prefix_length": 5,
              "fuzziness": 1
            }
          }
        },
        {
          "query_string": {
            "query": "name:elasticsearch"
          }
        }
      ],
      "must_not": [
        {
          "term": {
            "name": {
              "value": "meilisearch"
            }
          }
        }
      ]
    }
  }
}
`

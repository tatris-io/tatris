// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestQueryHandler(t *testing.T) {

	// prepare
	index, _, err := prepare.CreateIndexAndDocs(time.Now().Format(consts.VersionTimeFmt))
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

		c.Request.Body = io.NopCloser(bytes.NewBufferString(queryWithAggRequest))
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

const queryWithAggRequest = `
{
  "size": 20,
  "aggs": {
    "lang": {
     "terms": {
       "field": "lang",
       "size": 10
     },
     "aggs": {
       "avg_forks": {
         "avg": {
           "field": "forks"
         }
       },
       "sum_stars": {
         "sum": {
           "field": "stars"
         }
       },
       "cardinality_name": {
         "cardinality": {
           "field": "name"
         }
       },
      "range_forks": {
       "range": {
         "field": "forks",
         "ranges": [
           {
             "to": 10000
           },
           {
             "from": 10000,
             "to": 20000
           }
         ]
       }
      }
     }
    }
  }
}
`

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/test/ut/prepare"
	"go.uber.org/zap"
)

func TestQuerySingleIndex(t *testing.T) {

	// prepare
	index, _, err := prepare.CreateIndexAndDocs(time.Now().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("prepare index and docs fail: %s", err.Error())
	}

	for _, tt := range queryCases() {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.ReleaseMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			httpRequest := &http.Request{
				URL:    &url.URL{},
				Header: make(http.Header),
			}
			c.Request = httpRequest
			p := gin.Params{}
			p = append(p, gin.Param{Key: "index", Value: index.Name})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			c.Request.Body = io.NopCloser(bytes.NewBufferString(tt.req))
			QueryHandler(c)
			logger.Info(
				"test query handler",
				zap.String("name", tt.name),
				zap.Int("code", w.Code),
				zap.Any("resp", w.Body),
			)
			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestQueryMultipleIndexes(t *testing.T) {

	// prepare
	count := 5
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		versions[i] = time.Now().Format(time.RFC3339Nano)
		time.Sleep(time.Nanosecond * 1000)
	}
	indexes := make([]*core.Index, count)
	indexNames := make([]string, count)
	var err error
	for i := 0; i < count; i++ {
		indexes[i], _, err = prepare.CreateIndexAndDocs(versions[i])
		if err != nil {
			t.Fatalf("prepare index and docs fail: %s", err.Error())
		}
		indexNames[i] = indexes[i].Name
	}

	for _, tt := range queryCases() {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.ReleaseMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			httpRequest := &http.Request{
				URL:    &url.URL{},
				Header: make(http.Header),
			}
			c.Request = httpRequest
			p := gin.Params{}
			p = append(p, gin.Param{Key: "index", Value: strings.Join(indexNames, consts.Comma)})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			c.Request.Body = io.NopCloser(bytes.NewBufferString(tt.req))
			QueryHandler(c)
			logger.Info(
				"test multi query",
				zap.String("name", tt.name),
				zap.Int("code", w.Code),
				zap.Any("resp", w.Body),
			)
			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

type QueryCase struct {
	name string
	req  string
}

func queryCases() []QueryCase {
	return []QueryCase{
		{
			name: "query",
			req: `
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
                 }`,
		},
		{
			name: "agg",
			req: `
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
                 }`,
		},
		{
			name: "sort",
			req: `
                 {
                   "size": 20,
                   "query": {
                     "term": {
                       "lang": "Java"
                     }
                   },
                   "sort": [
                     {
                       "forks": {
                         "order": "desc"
                       },
                       "stars": {
                         "order": "asc"
                       }
                     }
                   ]
                 }`,
		},
		{
			name: "date_histogram",
			req: `
				{
				  "size": 0,
				  "aggs": {
					"histogram": {
					  "date_histogram": {
						"field": "start_time",
						"fixed_interval": "1m",
						"min_doc_count": 1,
						"extended_bounds": {
							"min": 1676513640000,
							"max": 1676600220000
						}
					}, 
					"aggs": {
						"sum_forks": {
						  "sum": {
							"field": "forks"
						  }
						}
 					}
				  }
				}
			  }
			`,
		},
	}
}

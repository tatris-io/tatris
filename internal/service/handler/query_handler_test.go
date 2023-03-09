// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/protocol"

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

	for _, tt := range createQueryCases(index.Name) {
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
			p = append(p, gin.Param{Key: "index", Value: tt.index})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			c.Request.Body = io.NopCloser(bytes.NewBufferString(tt.req))
			QueryHandler(c)
			logger.Info(
				"test query handler",
				zap.String("name", tt.name),
				zap.String("index", tt.index),
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
	for _, tt := range createQueryCases(indexNames...) {
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
			p = append(p, gin.Param{Key: "index", Value: tt.index})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			c.Request.Body = io.NopCloser(bytes.NewBufferString(tt.req))
			QueryHandler(c)
			logger.Info(
				"test multi query",
				zap.String("name", tt.name),
				zap.String("index", tt.index),
				zap.Int("code", w.Code),
				zap.Any("resp", w.Body),
			)
			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestAliasQuery(t *testing.T) {

	// prepare index and docs
	count := 5
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		versions[i] = time.Now().Format(time.RFC3339Nano)
		time.Sleep(time.Nanosecond * 1000)
	}
	indexes := make([]*core.Index, count)
	indexNames := make([]string, count)
	aliasNames := make([]string, 0)
	var err error
	for i := 0; i < count; i++ {
		indexes[i], _, err = prepare.CreateIndexAndDocs(versions[i])
		if err != nil {
			t.Fatalf("prepare index and docs fail: %s", err.Error())
		}
		indexNames[i] = indexes[i].Name
	}

	// prepare aliases
	t.Run("add_alias", func(t *testing.T) {
		actions := make([]protocol.Action, 0)
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				indexName := indexes[i].Name
				aliasName := fmt.Sprintf("alias_%s", versions[j])
				aliasNames = append(aliasNames, aliasName)
				actions = append(actions, map[string]*protocol.AliasTerm{
					"add": {
						Index: indexName,
						Alias: aliasName,
					},
				},
				)
			}
		}
		ManageAlias(t, actions)
	})

	// test
	for _, aliasName := range aliasNames {
		for _, tt := range createQueryCases(aliasName) {
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
				p = append(p, gin.Param{Key: "index", Value: tt.index})
				c.Params = p
				c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
				c.Request.Body = io.NopCloser(bytes.NewBufferString(tt.req))
				QueryHandler(c)
				logger.Info(
					"test alias query",
					zap.String("name", tt.name),
					zap.String("index", tt.index),
					zap.Int("code", w.Code),
					//zap.Any("resp", w.Body),
				)
				assert.Equal(t, http.StatusOK, w.Code)
			})
		}
	}

	// prepare aliases
	t.Run("remove_alias", func(t *testing.T) {
		actions := make([]protocol.Action, 0)
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				indexName := indexes[i].Name
				aliasName := fmt.Sprintf("alias_%s", versions[j])
				aliasNames = append(aliasNames, aliasName)
				actions = append(actions, map[string]*protocol.AliasTerm{
					"remove": {
						Index: indexName,
						Alias: aliasName,
					},
				},
				)
			}
		}
		ManageAlias(t, actions)
	})
}

type QueryCase struct {
	name  string
	index string
	req   string
}

func createQueryCases(names ...string) []QueryCase {
	queryCases := make([]QueryCase, 0)
	for _, c := range cases {
		queryCases = append(
			queryCases,
			QueryCase{name: c.name, index: strings.Join(names, consts.Comma), req: c.req},
		)
	}
	return queryCases
}

var cases = []QueryCase{
	{
		name: "query",
		req: `
			 {
			   "size": 20,
			   "from": 5,
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
		name: "range",
		req: `
		{
		    "size": 20,
            "from": 5,
		    "query": {
		        "range": {
		            "name": {
		                "gte": "a",
		                "lte": "b"
		            }
		        }
		    }
		}`,
	},
	{
		name: "agg",
		req: `
			 {
			   "size": 0,
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
				   },
				   "range_time": {
					"date_range": {
					  "field": "start_time",
					  "ranges": [
						{
						  "from": "1676513718560",
						  "to": "1676513918580"
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
                   "from": 5,
                   "query": {
                     "terms": {
                       "lang": ["Java", "C++"]
                     }
                   },
                   "sort": [
                     {
                       "forks": {
                         "order": "desc"
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
	{
		name: "histogram",
		req: `
			{
			  "size": 0,
			  "aggs": {
				"histogram": {
				  "histogram": {
					"field": "stars",
					"interval": 100,
					"min_doc_count": 0,
					"hard_bounds": {
						"min": 100,
						"max": 1000
					}
				}
			  }
			}
		  }
		`,
	},
	{
		name: "percentiles",
		req: `
			{
			  "size": 0,
			  "aggs": {
				"percentiles_stars": {
				  "percentiles": {
					"field": "stars",
					"percents": [
					  1,
					  5,
					  25,
					  50,
					  75,
					  95,
					  99
					]
				  }
				}
			  }
			}
			`,
	},
	{
		name: "filter",
		req: `
			{
			  "size": 0,
			  "aggs": {
				"filter_java": {
				  "filter": {
					"term": {
					  "lang": "Java"
					}
				  },
				  "aggs": {
					"sum_stars": {
					  "sum": {
						"field": "stars"
					  }
					}
				  },
                  "aggs": {
					"count_item": {
					  "count": {
						"field": "stars"
					  }
					}
				  }
				}
			  }
			}
			`,
	},
}

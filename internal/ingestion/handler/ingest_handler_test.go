// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/meta/handler"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestIngestHandler(t *testing.T) {

	t.Run("delete_index", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: "storage_product"})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		handler.DeleteIndexHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("create_index", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: "storage_product"})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewBufferString("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1},\"mappings\":{\"properties\":{\"name\":{\"type\":\"keyword\"},\"desc\":{\"type\":\"text\"}}}}"))
		handler.CreateIndexHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("ingest", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: "storage_product"})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewBufferString("{\"documents\":[{\"name\":\"tatris\",\"desc\":\"Time-aware storage and search system\"},{\"name\":\"mysql\",\"desc\":\"Relational database\"},{\"name\":\"elasticsearch\",\"desc\":\"Distributed, RESTful search and analytics engine\"},{\"name\":\"mongodb\",\"desc\":\"Source-available cross-platform document-oriented database program\"},{\"name\":\"redis\",\"desc\":\"Open source (BSD licensed), in-memory data structure store\"},{\"name\":\"hbase\",\"desc\":\"Distributed, scalable, big data store\"}]}"))
		IngestHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

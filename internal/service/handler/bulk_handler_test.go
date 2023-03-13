// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestBulkHandler(t *testing.T) {
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
		indexes[i], err = prepare.CreateIndex(versions[i])
		if err != nil {
			t.Fatalf("prepare index and docs fail: %s", err.Error())
		}
		indexNames[i] = indexes[i].Name
	}

	// test
	t.Run("test_bulk_handler", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "index", Value: indexNames[0]})
		ingestReq := protocol.IngestRequest{}
		_ = json.Unmarshal(bytes.NewBufferString(ingestRequest).Bytes(), &ingestReq)
		var bytesBuffer bytes.Buffer
		for _, index := range indexNames {
			bulkAction := make(map[string]protocol.BulkMeta, 0)
			bulkAction["create"] = protocol.BulkMeta{Index: index}
			bulkActionJSON, _ := json.Marshal(bulkAction)
			for _, document := range ingestReq.Documents {
				documentJSON, _ := json.Marshal(document)
				bytesBuffer.Write(bulkActionJSON)
				bytesBuffer.WriteString("\n")
				bytesBuffer.Write(documentJSON)
				bytesBuffer.WriteString("\n")
			}
		}
		c.Params = p
		c.Request.Header.Set("Content-Type", "text/plain;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewReader(bytesBuffer.Bytes()))
		BulkHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

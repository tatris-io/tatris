// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/test/prepare"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

const (
	indexPath     = "../../../test/materials/index.json"
	ingestReqPath = "../../../test/materials/ingest_request.json"
)

func TestIngestHandler(t *testing.T) {
	// prepare
	start := time.Now()
	version := start.Format(consts.VersionTimeFmt)
	index, err := prepare.PrepareIndex(indexPath, version)
	if err != nil {
		t.Fatalf("prepare index fail: %s", err.Error())
	}

	// test
	t.Run("test_ingest_handler", func(t *testing.T) {

		jsonFile, err := os.Open(ingestReqPath)
		if err != nil {
			t.Fatalf("open json file fail: %s", err.Error())
		}
		defer jsonFile.Close()
		jsonData, err := io.ReadAll(jsonFile)
		if err != nil {
			t.Fatalf("read json file fail: %s", err.Error())
		}
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
		c.Request.Body = io.NopCloser(bytes.NewBufferString(string(jsonData)))
		IngestHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

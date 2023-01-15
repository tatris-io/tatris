// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/protocol"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

const (
	indexPath = "../../../test/materials/index.json"
)

func TestIndexHandler(t *testing.T) {

	jsonFile, err := os.Open(indexPath)
	if err != nil {
		t.Fatalf("open json file fail: %s", err.Error())
	}
	defer jsonFile.Close()
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		t.Fatalf("read json file fail: %s", err.Error())
	}
	index := &protocol.Index{}
	version := time.Now().Format(consts.VersionTimeFmt)
	index.Name = fmt.Sprintf("%s_%s", index.Name, version)
	json.Unmarshal(jsonData, &index)

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
		p = append(p, gin.Param{Key: "index", Value: index.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		c.Request.Body = io.NopCloser(bytes.NewBufferString(string(jsonData)))
		CreateIndexHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("get_index", func(t *testing.T) {
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
		GetIndexHandler(c)
		getIndex := protocol.Index{}
		json.Unmarshal(w.Body.Bytes(), &getIndex)
		assert.Equal(t, index.Name, getIndex.Name)
		assert.Equal(t, index.Settings.NumberOfShards, getIndex.Settings.NumberOfShards)
		assert.Equal(t, index.Settings.NumberOfReplicas, getIndex.Settings.NumberOfReplicas)
		for field, prop := range index.Mappings.Properties {
			assert.Equal(t, getIndex.Mappings.Properties[field], prop)
		}
		fmt.Println(index)
		assert.Equal(t, http.StatusOK, w.Code)
	})

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
		p = append(p, gin.Param{Key: "index", Value: index.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		DeleteIndexHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

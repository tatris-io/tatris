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

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestAliasHandler(t *testing.T) {

	// prepare
	count := 10
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		versions[i] = time.Now().Format(time.RFC3339Nano)
		time.Sleep(time.Nanosecond * 1000)
	}
	indexes := make([]*core.Index, count)
	var err error
	for i := 0; i < count; i++ {
		indexes[i], err = prepare.CreateIndex(versions[i])
		if err != nil {
			t.Fatalf("prepare index fail: %s", err.Error())
		}
	}
	// test
	t.Run("add_alias", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		aliasManageRequest := protocol.AliasManageRequest{Actions: make([]protocol.Action, 0)}
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				aliasManageRequest.Actions = append(
					aliasManageRequest.Actions,
					map[string]*protocol.AliasTerm{
						"add": {
							Index: indexes[i].Name,
							Alias: fmt.Sprintf("alias_%s", versions[j]),
						},
					},
				)
			}
		}
		requestJSON, err := json.Marshal(aliasManageRequest)
		assert.NoError(t, err)
		c.Request.Body = io.NopCloser(bytes.NewBufferString(string(requestJSON)))
		ManageAliasHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("get_by_index", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		for i := 0; i < count; i++ {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := &http.Request{
				URL:    &url.URL{},
				Header: make(http.Header),
			}
			c.Request = req
			p := gin.Params{}
			p = append(p, gin.Param{Key: "index", Value: indexes[i].Name})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			GetAliasHandler(c)
			logger.Info(
				"get alias response",
				zap.Int("code", w.Code),
				zap.String("body", w.Body.String()),
			)
			assert.Equal(t, http.StatusOK, w.Code)
			respData, err := io.ReadAll(w.Body)
			assert.NoError(t, err)
			aliasGetResponse := protocol.AliasGetResponse{}
			err = json.Unmarshal(respData, &aliasGetResponse)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(aliasGetResponse))
			aliases := aliasGetResponse[indexes[i].Name]
			assert.NotNil(t, aliases)
			assert.Equal(t, i+1, len(aliases.Aliases))
		}
	})

	t.Run("get_by_alias", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		for i := 0; i < count; i++ {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := &http.Request{
				URL:    &url.URL{},
				Header: make(http.Header),
			}
			c.Request = req
			p := gin.Params{}
			p = append(p, gin.Param{Key: "alias", Value: fmt.Sprintf("alias_%s", versions[i])})
			c.Params = p
			c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
			GetAliasHandler(c)
			logger.Info("get alias response", zap.Any("resp", w))
			assert.Equal(t, http.StatusOK, w.Code)
			respData, err := io.ReadAll(w.Body)
			assert.NoError(t, err)
			aliasGetResponse := protocol.AliasGetResponse{}
			err = json.Unmarshal(respData, &aliasGetResponse)
			assert.NoError(t, err)
			assert.Equal(t, count-i, len(aliasGetResponse))
			aliases := aliasGetResponse[indexes[i].Name]
			assert.NotNil(t, aliases)
			assert.Equal(t, 1, len(aliases.Aliases))
		}
	})

	t.Run("get_by_index_and_alias", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				req := &http.Request{
					URL:    &url.URL{},
					Header: make(http.Header),
				}
				c.Request = req
				p := gin.Params{}
				p = append(p, gin.Param{Key: "index", Value: indexes[i].Name})
				p = append(p, gin.Param{Key: "alias", Value: fmt.Sprintf("alias_%s", versions[j])})
				c.Params = p
				c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
				GetAliasHandler(c)
				logger.Info("get alias response", zap.Any("resp", w))
				assert.Equal(t, http.StatusOK, w.Code)
				respData, err := io.ReadAll(w.Body)
				assert.NoError(t, err)
				aliasGetResponse := protocol.AliasGetResponse{}
				err = json.Unmarshal(respData, &aliasGetResponse)
				assert.NoError(t, err)
				assert.Equal(t, 1, len(aliasGetResponse))
				aliases := aliasGetResponse[indexes[i].Name]
				assert.NotNil(t, aliases)
				assert.Equal(t, 1, len(aliases.Aliases))
			}
		}
	})

	t.Run("remove_alias", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		aliasManageRequest := protocol.AliasManageRequest{Actions: make([]protocol.Action, 0)}
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				if i%2 == 0 && j%2 == 0 {
					aliasManageRequest.Actions = append(
						aliasManageRequest.Actions,
						map[string]*protocol.AliasTerm{
							"remove": {
								Index: indexes[i].Name,
								Alias: fmt.Sprintf("alias_%s", versions[j]),
							},
						},
					)
				}
			}
		}
		requestJSON, err := json.Marshal(aliasManageRequest)
		assert.NoError(t, err)
		c.Request.Body = io.NopCloser(bytes.NewBufferString(string(requestJSON)))
		ManageAliasHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("get_by_index_and_alias_after_remove", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		for i := 0; i < count; i++ {
			for j := 0; j <= i; j++ {
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				req := &http.Request{
					URL:    &url.URL{},
					Header: make(http.Header),
				}
				c.Request = req
				p := gin.Params{}
				p = append(p, gin.Param{Key: "index", Value: indexes[i].Name})
				p = append(p, gin.Param{Key: "alias", Value: fmt.Sprintf("alias_%s", versions[j])})
				c.Params = p
				c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
				GetAliasHandler(c)
				logger.Info("get alias response", zap.Any("resp", w))
				if i%2 == 0 && j%2 == 0 {
					assert.Equal(t, http.StatusNotFound, w.Code)
				} else {
					assert.Equal(t, http.StatusOK, w.Code)
					respData, err := io.ReadAll(w.Body)
					assert.NoError(t, err)
					aliasGetResponse := protocol.AliasGetResponse{}
					err = json.Unmarshal(respData, &aliasGetResponse)
					assert.NoError(t, err)
					assert.Equal(t, 1, len(aliasGetResponse))
					aliases := aliasGetResponse[indexes[i].Name]
					assert.NotNil(t, aliases)
					assert.Equal(t, 1, len(aliases.Aliases))
				}
			}
		}
	})
}

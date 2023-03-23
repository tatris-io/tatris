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
	"strings"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestIndexTemplateHandler(t *testing.T) {

	template, err := prepare.GetIndexTemplate(
		strings.ReplaceAll(
			time.Now().Format(consts.TimeFmtWithoutSeparator),
			consts.Dot,
			consts.Empty,
		),
	)
	if err != nil {
		t.Fatalf("prepare template fail: %s", err.Error())
	}

	t.Run("create_index_template", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "template", Value: template.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		templateBytes, err := json.Marshal(template)
		if err != nil {
			t.Fatalf("parse template fail: %s", err.Error())
		}
		c.Request.Body = io.NopCloser(bytes.NewBufferString(string(templateBytes)))
		CreateIndexTemplateHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("index_template_exist_Y", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "template", Value: template.Name})
		c.Params = p
		IndexTemplateExistHandler(c)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("get_index_template", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "template", Value: template.Name})
		c.Params = p
		GetIndexTemplateHandler(c)
		templateResponse := protocol.IndexTemplateResponse{}
		json.Unmarshal(w.Body.Bytes(), &templateResponse)
		assert.Equal(t, 1, len(templateResponse.IndexTemplates))
		assert.Equal(t, template.Name, templateResponse.IndexTemplates[0].Name)
		assert.Equal(
			t,
			template.Template.Settings.NumberOfShards,
			templateResponse.IndexTemplates[0].IndexTemplate.Template.Settings.NumberOfShards,
		)
		assert.Equal(
			t,
			template.Template.Settings.NumberOfReplicas,
			templateResponse.IndexTemplates[0].IndexTemplate.Template.Settings.NumberOfReplicas,
		)
		for field, prop := range template.Template.Mappings.Properties {
			assert.Equal(
				t,
				templateResponse.IndexTemplates[0].IndexTemplate.Template.Mappings.Properties[field].Type,
				prop.Type,
			)
		}
		fmt.Println(template)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("delete_index_template", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "template", Value: template.Name})
		c.Params = p
		c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
		DeleteIndexTemplateHandler(c)
		fmt.Println(w)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("index_template_exist_N", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		p := gin.Params{}
		p = append(p, gin.Param{Key: "template", Value: template.Name})
		c.Params = p
		IndexTemplateExistHandler(c)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

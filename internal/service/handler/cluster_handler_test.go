// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
)

func TestClusterStatus(t *testing.T) {

	t.Run("cluster_status", func(t *testing.T) {
		gin.SetMode(gin.ReleaseMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		req := &http.Request{
			URL:    &url.URL{},
			Header: make(http.Header),
		}
		c.Request = req
		c.Params = gin.Params{}
		ClusterStatusHandler(c)
		assert.Equal(t, http.StatusOK, w.Code)
		clusterStatus := protocol.ClusterStatus{}
		json.Unmarshal(w.Body.Bytes(), &clusterStatus)
		assert.Equal(t, consts.StatusGreen, clusterStatus.Status)
	})
}

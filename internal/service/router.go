// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package service

import (
	"fmt"

	handler1 "github.com/tatris-io/tatris/internal/service/handler"

	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log/logger"
)

func StartHTTPServer(roles ...string) {
	if len(roles) == 0 {
		roles = []string{"all"}
	}

	router := gin.New()

	router.NoRoute(func(context *gin.Context) {
		msg := fmt.Sprintf("route not found: %v", context.Request.RequestURI)
		logger.Error(msg)
		context.String(404, msg)
	})
	router.NoMethod(func(context *gin.Context) {
		msg := fmt.Sprintf("method not found: %v", context.Request.RequestURI)
		logger.Error(msg)
		context.String(404, msg)
	})
	router.Use(AccessLog())
	router.Use(gin.Recovery())

	// api version: v1
	v1 := router.Group("/v1")

	for _, role := range roles {
		switch role {
		case "ingestion":
			registerIngestion(v1)
		case "query":
			registerQuery(v1)
		case "meta":
			registerMeta(v1)
		case "all":
			registerIngestion(v1)
			registerQuery(v1)
			registerMeta(v1)
		default:
		}
	}

	if err := router.Run(":8080"); err != nil {
		logger.Error(
			"Tatris HTTP server start failed",
			zap.Any("roles", roles),
			zap.String("msg", err.Error()),
		)
	}
}

func registerIngestion(group *gin.RouterGroup) {
	logger.Info("ingestion APIs registering")
	group.PUT("/:index/_ingest", handler1.IngestHandler)
}

func registerQuery(group *gin.RouterGroup) {
	logger.Info("query APIs registering")
	group.POST("/:index/_search", handler1.QueryHandler)
}

func registerMeta(group *gin.RouterGroup) {
	logger.Info("meta APIs registering")
	group.PUT("/:index", handler1.CreateIndexHandler)
	group.GET("/:index", handler1.GetIndexHandler)
	group.DELETE("/:index", handler1.DeleteIndexHandler)
	group.HEAD("/:index", handler1.IndexExistHandler)
}

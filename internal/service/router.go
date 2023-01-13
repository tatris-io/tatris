// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package service

import (
	"github.com/gin-gonic/gin"
	handler1 "github.com/tatris-io/tatris/internal/ingestion/handler"
	handler2 "github.com/tatris-io/tatris/internal/meta/handler"
	handler3 "github.com/tatris-io/tatris/internal/query/handler"
)

func StartHTTPServer(roles ...string) {
	if len(roles) == 0 {
		roles = []string{"all"}
	}

	router := gin.New()

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

	router.NoRoute(func(context *gin.Context) {
		context.String(404, "router not found")
	})
	router.NoMethod(func(context *gin.Context) {
		context.String(404, "method not found")
	})

	router.Use(gin.Recovery())
	if err := router.Run(":8080"); err != nil {
		print("http server start failed, roles=%s\n", roles)
	}
}

func registerIngestion(group *gin.RouterGroup) {
	group.PUT("/:index/_ingest", handler1.IngestHandler)
}

func registerQuery(group *gin.RouterGroup) {
	group.POST("/:index/_search", handler3.QueryHandler)
}

func registerMeta(group *gin.RouterGroup) {
	group.PUT("/:index", handler2.CreateIndexHandler)
	group.GET("/:index", handler2.GetIndexHandler)
	group.DELETE("/:index", handler2.DeleteIndexHandler)
}

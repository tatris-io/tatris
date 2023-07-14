// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package service

import (
	"fmt"

	"github.com/gin-contrib/pprof"

	"github.com/tatris-io/tatris/internal/service/handler"

	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log/logger"
)

func StartHTTPServer(roles ...string) {
	if len(roles) == 0 {
		roles = []string{"all"}
	}

	router := gin.New()

	pprof.Register(router)

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
	router.Use(addResponseHeader())

	routerGroup := router.Group("")

	for _, role := range roles {
		switch role {
		case "ingestion":
			registerIngestion(routerGroup)
		case "query":
			registerQuery(routerGroup)
		case "meta":
			registerMeta(routerGroup)
		case "all":
			registerIngestion(routerGroup)
			registerQuery(routerGroup)
			registerMeta(routerGroup)
		default:
		}
	}

	if err := router.Run(":6060"); err != nil {
		logger.Error(
			"Tatris HTTP server start failed",
			zap.Any("roles", roles),
			zap.String("msg", err.Error()),
		)
	}
}

func registerIngestion(group *gin.RouterGroup) {
	logger.Info("ingestion APIs registering")
	group.PUT("/:index/_ingest", handler.IngestHandler)
	group.POST("/:index/_ingest", handler.IngestHandler)
	group.PUT("/:index/_bulk", handler.BulkHandler)
	group.POST("/:index/_bulk", handler.BulkHandler)
	group.PUT("/_bulk", handler.BulkHandler)
	group.POST("/_bulk", handler.BulkHandler)
}

func registerQuery(group *gin.RouterGroup) {
	logger.Info("query APIs registering")

	group.POST("/:index/_search", handler.QueryHandler)
	group.GET("/:index/_search", handler.QueryHandler)
}

func registerMeta(group *gin.RouterGroup) {
	logger.Info("meta APIs registering")

	group.GET("/", handler.ClusterInfoHandler)
	group.GET("/_nodes", handler.ClusterNodesInfoHandler)
	group.GET("/_nodes/_all", handler.ClusterNodesInfoHandler)

	group.GET("/_cluster/health", handler.ClusterStatusHandler)
	group.GET("/_cluster/health/:index", handler.ClusterStatusHandler)

	group.PUT("/:index", handler.CreateIndexHandler)
	group.POST("/:index", handler.CreateIndexHandler)
	group.GET("/:index", handler.GetIndexHandler)
	group.DELETE("/:index", handler.DeleteIndexHandler)
	group.HEAD("/:index", handler.IndexExistHandler)

	group.PUT("/_indices/:index", handler.CreateIndexHandler)
	group.POST("/_indices/:index", handler.CreateIndexHandler)
	group.GET("/_indices/:index", handler.GetIndexHandler)
	group.DELETE("/_indices/:index", handler.DeleteIndexHandler)
	group.HEAD("/_indices/:index", handler.IndexExistHandler)

	group.PUT("/_aliases", handler.ManageAliasHandler)
	group.POST("/_aliases", handler.ManageAliasHandler)
	group.GET("/_aliases", handler.GetAliasHandler)
	group.GET("/:index/_aliases", handler.GetAliasHandler)
	group.GET("/_aliases/:alias", handler.GetAliasHandler)
	group.GET("/:index/_aliases/:alias", handler.GetAliasHandler)

	group.PUT("/_alias", handler.ManageAliasHandler)
	group.POST("/_alias", handler.ManageAliasHandler)
	group.GET("/_alias", handler.GetAliasHandler)
	group.GET("/:index/_alias", handler.GetAliasHandler)
	group.GET("/_alias/:alias", handler.GetAliasHandler)
	group.GET("/:index/_alias/:alias", handler.GetAliasHandler)

	group.PUT("/_index_template/:template", handler.CreateIndexTemplateHandler)
	group.POST("/_index_template/:template", handler.CreateIndexTemplateHandler)
	group.GET("/_index_template/:template", handler.GetIndexTemplateHandler)
	group.DELETE("/_index_template/:template", handler.DeleteIndexTemplateHandler)
	group.HEAD("/_index_template/:template", handler.IndexTemplateExistHandler)
}

func addResponseHeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		// The client validating if the response has X-Elastic-Product=Elasticsearch header
		c.Writer.Header().Set("X-elastic-product", "Elasticsearch")
		c.Next()
	}
}

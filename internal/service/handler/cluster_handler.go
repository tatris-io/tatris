// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/protocol"
)

// ClusterStatusHandler is used to view the status of the cluster.
// Right now this is a pseudo-implementation that the started cluster is always considered healthy
// until we support cluster mode.
func ClusterStatusHandler(c *gin.Context) {
	OK(c, protocol.ClusterStatus{
		ClusterName:                 "docker-cluster",
		Status:                      consts.StatusGreen,
		TimedOut:                    false,
		NumberOfNodes:               1,
		NumberOfDataNodes:           1,
		ActivePrimaryShards:         1,
		ActiveShards:                1,
		RelocationShards:            0,
		InitializingShards:          0,
		UnassignedShards:            0,
		DelayedUnassignedShards:     0,
		NumberOfPendingTasks:        0,
		NumberOfInFlightFetch:       0,
		TaskMaxWaitingInQueueMills:  0,
		ActiveShardsPercentAsNumber: 100,
	})
}

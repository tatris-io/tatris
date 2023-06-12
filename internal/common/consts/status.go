// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package consts

// The index level status is controlled by the worst shard status. The cluster status is controlled
// by the worst index status.
// On the shard level:
const (
	// StatusGreen means that all shards are allocated.
	StatusGreen = "green"
	// StatusYellow means that the primary shard is allocated but replicas are not.
	StatusYellow = "yellow"
	// StatusRed indicates that the specific shard is not allocated in the cluster.
	StatusRed = "red"
)

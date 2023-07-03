// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type ClusterStatus struct {
	ClusterName                 string  `json:"cluster_name"`
	Status                      string  `json:"status"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               int     `json:"number_of_nodes"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes"`
	ActivePrimaryShards         int     `json:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards"`
	RelocationShards            int     `json:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMills  int64   `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
}

type ClusterInfo struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	ClusterUUID string `json:"cluster_uuid"`
	// version represents the elasticsearch version, we cannot modify the structure of
	// the returned body due to the need to be compatible with the elasticsearch client
	Version       VersionInfo `json:"version"`
	TatrisVersion VersionInfo `json:"tatris_version"`
	Tagline       string      `json:"tagline"`
}

type ClusterNodesInfo struct {
	Nodes ClusterNodes `json:"nodes"`
}

type ClusterNodes map[string]ClusterNode

type ClusterNode struct {
	Name          string `json:"name"`
	IP            string `json:"ip"`
	Host          string `json:"host"`
	Version       string `json:"version"`
	TatrisVersion string `json:"tatris_version"`
}

type VersionInfo struct {
	Number                    string `json:"number"`
	BuildFlavor               string `json:"build_flavor"`
	BuildHash                 string `json:"build_hash"`
	BuildDate                 string `json:"build_date"`
	BuildSnapshot             bool   `json:"build_snapshot"`
	MinimumWireVersion        string `json:"minimum_wire_version"`
	MinimumIndexCompatibility string `json:"minimum_index_compatibility"`
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type Index struct {
	// index name
	Name string `json:"name"`
	// index settings
	Settings *Settings `json:"settings,omitempty"`
	// index mappings
	Mappings *Mappings `json:"mappings,omitempty"`
}

type Settings struct {
	// number of shards, default is 1
	NumberOfShards int `json:"number_of_shards,omitempty"`
	// number of replicas, default is 1 (ie one replica for each primary shard)
	NumberOfReplicas int `json:"number_of_replicas,omitempty"`
}

// Mappings Mapping is the process of defining how a document, and the fields it contains, are
// stored and indexed.
type Mappings struct {
	// Mapping mode, true means Tatris will define the field type dynamically, otherwise user
	// need to specify the field type in index. It is true by default.
	Dynamic bool `json:"dynamic mappings,omitempty"`
	// The default value `ignore` by default means that when the field check fails, we simply
	// ignore the field, and `abort` means the doc is rejected.
	RejectedPolicy string `json:"rejected policy,omitempty"`
	// Type mappings, object fields and nested fields contain sub-fields, called properties.
	Properties map[string]Property `json:"properties,omitempty"`
}

type Property struct {
	// field data type
	Type string
}

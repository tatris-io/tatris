// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package meta

type Index struct {
	// index name
	Name string `json:"name"`
	// index settings
	Settings *Settings `json:"settings,omitempty"`
	// index mappings
	Mappings *Mappings `json:"mappings,omitempty"`
	// index shards
	Shards []Shard `json:"shards"`
}

type Settings struct {
	// number of shards, default is 1
	NumberOfShards int32 `json:"number_of_shards,omitempty"`
	// number of replicas, default is 1 (ie one replica for each primary shard)
	NumberOfReplicas int32 `json:"number_of_replicas,omitempty"`
}

// Mappings Mapping is the process of defining how a document, and the fields it contains, are stored and indexed.
type Mappings struct {
	// Type mappings, object fields and nested fields contain sub-fields, called properties.
	Properties map[string]Property `json:"properties,omitempty"`
}

type Property struct {
	// field data type
	Type string
}

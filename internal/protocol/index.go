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
	// Mapping mode:
	// `true` means Tatris will define the field type dynamically, new fields are added to the
	// mapping (default).
	// `false` means new fields are ignored. These fields will not be indexed or
	// searchable, but will still appear in the _source field of returned hits. These fields will
	// not be added to the mapping, and new fields must be added explicitly.
	// `strict` means if new fields are detected, an exception is thrown and the document is
	// rejected. New fields must be explicitly added to the mapping.
	Dynamic string `json:"dynamic,omitempty"`
	// Type mappings, object fields and nested fields contain sub-fields, called properties.
	Properties map[string]Property `json:"properties,omitempty"`
}

type Property struct {
	// field data type
	Type string
	// field-level mapping mode
	Dynamic string `json:"dynamic,omitempty"`
}

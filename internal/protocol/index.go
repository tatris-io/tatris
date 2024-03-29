// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type CreateIndexResponse struct {
	*Response
	ShardsAcknowledged bool   `json:"shards_acknowledged,string,omitempty"`
	Index              string `json:"index,omitempty"`
}

type Index struct {
	// index name
	Name string `json:"name"`
	// index settings
	Settings *Settings `json:"settings,omitempty"`
	// index mappings
	Mappings *Mappings `json:"mappings,omitempty"`
}

// Settings contains index-level settings that can be set per-index.
// The JSON unmarshalling of Settings is redefined in UnmarshalJSON.
type Settings struct {
	// number of shards, default is 1
	NumberOfShards int `json:"number_of_shards,omitempty"`
	// number of replicas, default is 1 (ie one replica for each primary shard)
	NumberOfReplicas int `json:"number_of_replicas,omitempty"`
}

// Mappings is the process of defining how a document, and the fields it contains, are
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
	// DynamicTemplates allow you greater control of how Tatris maps your data beyond the default
	// dynamic field mapping rules.
	// You enable dynamic mapping by setting the Dynamic mode to true or runtime.
	DynamicTemplates []map[string]*DynamicTemplate `json:"dynamic_templates,omitempty"`
	// Type mappings, object fields and nested fields contain subfields, called properties.
	Properties map[string]*Property `json:"properties,omitempty"`
}

type DynamicTemplate struct {
	Mapping          *DynamicTemplateMapping `json:"mapping"`
	MatchMappingType string                  `json:"match_mapping_type"`
	MatchPattern     string                  `json:"match_pattern"`
	Match            string                  `json:"match"`
	Unmatch          string                  `json:"unmatch"`
	// TODO: PathMatch and PathUnmatch will be enabled after nested types are supported
	PathMatch   string `json:"path_match"`
	PathUnmatch string `json:"path_unmatch"`
}

type DynamicTemplateMapping struct {
	Type string `json:"type"`
}

type Property struct {
	// field data type
	Type string `json:"type,omitempty"`
	// field-level mapping mode
	Dynamic string `json:"dynamic,omitempty"`
}

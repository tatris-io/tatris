// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type IndexTemplate struct {
	// Name of the index template.
	Name string `json:"name"`
	// Priority to determine index template precedence when a new data stream or index is created. The index template with the highest priority is chosen.
	// If no priority is specified the template is treated as though it is of priority 0 (the lowest priority).
	Priority int `json:"priority"`
	// IndexPatterns are used to match the names of indices during creation.
	IndexPatterns []string `json:"index_patterns"`
	// Template to be applied.
	Template *Template `json:"template"`
}

type Template struct {
	Settings *Settings `json:"settings"`
	Mappings *Mappings `json:"mappings"`
	Aliases  *Aliases  `json:"aliases"`
}

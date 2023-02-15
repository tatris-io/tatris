// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type AliasManageRequest struct {
	Actions []Action `json:"actions"`
}

type Action map[string]*AliasTerm

type AliasTerm struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

type AliasGetResponse map[string]*Aliases

type Aliases struct {
	Aliases map[string]*AliasTerm `json:"aliases"`
}

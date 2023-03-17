// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type Response struct {
	Acknowledged bool   `json:"acknowledged,string,omitempty"`
	Error        *Error `json:"error,omitempty"`
}

type Error struct {
	*Err
	RootCause *Err `json:"root_cause,omitempty"`
}

type Err struct {
	Type         string `json:"type,omitempty"`
	Reason       string `json:"reason,omitempty"`
	ResourceType string `json:"resource.type,omitempty"`
	ResourceID   string `json:"resource.id,omitempty"`
	IndexUUID    string `json:"index_uuid,omitempty"`
	Index        string `json:"index,omitempty"`
}

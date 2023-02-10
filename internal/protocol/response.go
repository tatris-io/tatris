// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type Response struct {
	Code    int    `json:"code"`
	Data    any    `json:"data"`
	Err     error  `json:"err"`
	Message string `json:"message"`
}

// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package errs defines error details
package errs

import (
	"errors"
	"fmt"
)

var (
	ErrEmptySettings      = errors.New("empty settings")
	ErrEmptyMappings      = errors.New("empty mappings")
	ErrNoSegmentMatched   = errors.New("no segment matched")
	ErrIndexLibNotSupport = errors.New("index lib not support")
	ErrSpecifyDirAsFile   = errors.New("specify directory as file")
	ErrSegmentReadonly    = errors.New("segment is readonly")
	ErrEmptyField         = errors.New("invalid field specified, must be non-null and non-empty")
)

func IsIndexNotFound(err error) bool {
	var notFoundErr *IndexNotFoundError
	return err != nil && errors.As(err, &notFoundErr)
}

type IndexNotFoundError struct {
	Index string `json:"index"`
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index: %s", e.Index)
}

func IsShardNotFound(err error) bool {
	var notFoundErr *ShardNotFoundError
	return err != nil && errors.As(err, &notFoundErr)
}

type ShardNotFoundError struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
}

func (e *ShardNotFoundError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d", e.Index, e.Shard)
}

type NoShardError struct {
	Index string `json:"index"`
}

func (e *NoShardError) Error() string {
	return fmt.Sprintf("index: %s", e.Index)
}

type SegmentNotFoundError struct {
	Index   string `json:"index"`
	Shard   int    `json:"shard"`
	Segment int    `json:"segment"`
}

func (e *SegmentNotFoundError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d, segment: %d", e.Index, e.Shard, e.Segment)
}

func IsIndexTemplateNotFound(err error) bool {
	var notFoundErr *IndexTemplateNotFoundError
	return err != nil && errors.As(err, &notFoundErr)
}

type IndexTemplateNotFoundError struct {
	IndexTemplate string `json:"index_template"`
}

func (e *IndexTemplateNotFoundError) Error() string {
	return fmt.Sprintf("index_template: %s", e.IndexTemplate)
}

type NoSegmentError struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
}

func (e *NoSegmentError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d", e.Index, e.Shard)
}

type InvalidFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("field: %s, message: %s", e.Field, e.Message)
}

type InvalidFieldValError struct {
	Field string `json:"field"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (e *InvalidFieldValError) Error() string {
	return fmt.Sprintf("field: %s, type: %s, value: %v", e.Field, e.Type, e.Value)
}

type UnsupportedError struct {
	Desc  string `json:"desc"`
	Value any    `json:"value"`
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("desc: %s, value: %v", e.Desc, e.Value)
}

type InvalidRangeError struct {
	Desc         string `json:"desc"`
	Value        any    `json:"value"`
	Left         any    `json:"left"`
	LeftExclude  bool   `json:"left_exclude"`
	Right        any    `json:"right"`
	RightExclude bool   `json:"right_exclude"`
}

func (e *InvalidRangeError) Error() string {
	lParenthesis := "["
	if e.LeftExclude {
		lParenthesis = "("
	}
	rParenthesis := "["
	if e.RightExclude {
		rParenthesis = "("
	}
	return fmt.Sprintf(
		"invalid %s: %v, should between %s%v, %v%s",
		e.Desc,
		e.Value,
		lParenthesis,
		e.Left,
		e.Right,
		rParenthesis,
	)
}

type InvalidQueryError struct {
	Message string `json:"message"`
	Query   any    `json:"query"`
}

func (e *InvalidQueryError) Error() string {
	return fmt.Sprintf("message: %s, query: %v", e.Message, e.Query)
}

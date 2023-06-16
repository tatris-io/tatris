// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package errs defines error details
package errs

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidJSONType = errors.New(
		"invalid JSON type, possible values are [string, long, double, boolean, date]",
	)
	ErrNoMappingInDynamicTemplate = errors.New("dynamic template must have a mapping")
	ErrEmptySettings              = errors.New("empty settings")
	ErrEmptyMappings              = errors.New("empty mappings")
	ErrNoSegmentMatched           = errors.New("no segment matched")
	ErrIndexLibNotSupport         = errors.New("index lib not support")
	ErrSpecifyDirAsFile           = errors.New("specify directory as file")
	ErrSegmentReadonly            = errors.New("segment is readonly")
	ErrEmptyField                 = errors.New(
		"invalid field specified, must be non-null and non-empty",
	)
)

func IndexNotFound(err error) (bool, *IndexNotFoundError) {
	var notFoundErr *IndexNotFoundError
	return err != nil && errors.As(err, &notFoundErr), notFoundErr
}

func IsIndexNotFound(err error) bool {
	var notFoundErr *IndexNotFoundError
	return err != nil && errors.As(err, &notFoundErr)
}

type IndexNotFoundError struct {
	Index string `json:"index"`
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index not found: %s", e.Index)
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
	return fmt.Sprintf("shard not found: %s/%d", e.Index, e.Shard)
}

type NoShardError struct {
	Index string `json:"index"`
}

func (e *NoShardError) Error() string {
	return fmt.Sprintf("index has no shards: %s", e.Index)
}

type SegmentNotFoundError struct {
	Index   string `json:"index"`
	Shard   int    `json:"shard"`
	Segment int    `json:"segment"`
}

func (e *SegmentNotFoundError) Error() string {
	return fmt.Sprintf("segment not found: %s/%d/%d", e.Index, e.Shard, e.Segment)
}

func IndexTemplateNotFound(err error) (bool, *IndexTemplateNotFoundError) {
	var notFoundErr *IndexTemplateNotFoundError
	return err != nil && errors.As(err, &notFoundErr), notFoundErr
}

func IsIndexTemplateNotFound(err error) bool {
	var notFoundErr *IndexTemplateNotFoundError
	return err != nil && errors.As(err, &notFoundErr)
}

type IndexTemplateNotFoundError struct {
	IndexTemplate string `json:"index_template"`
}

func (e *IndexTemplateNotFoundError) Error() string {
	return fmt.Sprintf("index_template not found: %s", e.IndexTemplate)
}

type NoSegmentError struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
}

func (e *NoSegmentError) Error() string {
	return fmt.Sprintf("shard has no segments: %s/%d", e.Index, e.Shard)
}

type InvalidFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("invalid field: %s, %s", e.Field, e.Message)
}

type InvalidFieldValError struct {
	Field string `json:"field"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (e *InvalidFieldValError) Error() string {
	return fmt.Sprintf("invalid field value for %s: %s, %v ", e.Type, e.Field, e.Value)
}

type InvalidAggFieldTypeError struct {
	Field           string `json:"field"`
	FieldType       string `json:"type"`
	AggregationType string `json:"aggregation_type"`
	AggregationName string `json:"aggregation_name"`
}

func (e *InvalidAggFieldTypeError) Error() string {
	return fmt.Sprintf(
		"field [%s] of type [%s] is not supported for %s aggregation [%s]",
		e.Field,
		e.FieldType,
		e.AggregationType,
		e.AggregationName,
	)
}

type UnsupportedError struct {
	Desc  string `json:"desc"`
	Value any    `json:"value"`
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("unsupported: %s, %v", e.Desc, e.Value)
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
	return fmt.Sprintf("invalid query for %s: %v", e.Message, e.Query)
}

type InvalidBulkError struct {
	Message string `json:"message"`
}

func (e *InvalidBulkError) Error() string {
	return fmt.Sprintf("invalid bulk request: %s", e.Message)
}

type InvalidResourceNameError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

func (e *InvalidResourceNameError) Error() string {
	return fmt.Sprintf("invalid resource name: %s, %s", e.Name, e.Message)
}

func IsInvalidResourceNameError(err error) bool {
	var invalidResourceNameErr *InvalidResourceNameError
	return err != nil && errors.As(err, &invalidResourceNameErr)
}

type QueryLoadExceedError struct {
	Indexes []string `json:"indexes"`
	Message string   `json:"message"`
	Query   any      `json:"query"`
}

func (e *QueryLoadExceedError) Error() string {
	return fmt.Sprintf("query load exceeded: %v, %s: %v", e.Indexes, e.Message, e.Query)
}

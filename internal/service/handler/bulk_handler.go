// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
)

// maxBytesOfLine limits the maximum bytes that can be read for each line of the bulk request
const maxBytesOfLine = 1024 * 1024 * 4

func BulkHandler(c *gin.Context) {
	name := c.Param("index")
	var index *core.Index
	var err error
	start := time.Now()
	code := http.StatusOK
	response := protocol.Response{}
	documents, err := divideBulk(name, c.Request.Body)
	if err != nil {
		code = http.StatusBadRequest
		response.Error = true
		response.Message = err.Error()
	} else {
		for idx, docs := range documents {
			if index, err = metadata.GetIndexExplicitly(idx); err != nil {
				if errs.IsIndexNotFound(err) {
					// create the index if it does not exist
					index = &core.Index{Index: &protocol.Index{Name: idx}}
					err = metadata.CreateIndex(index)
				}
				if err != nil {
					code = http.StatusInternalServerError
					response.Error = true
					response.Message = err.Error()
					break
				}
			}
			if err = ingestion.IngestDocs(index, docs); err != nil {
				code = http.StatusInternalServerError
				response.Error = true
				response.Message = err.Error()
				break
			}
		}
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)
}

// divideBulk groups the documents in the bulk request by index and returns them.
// Note that Tatris is currently designed not to allow modification of ingested
// documents, which means that only the operation CREATE is legal, so we do not need to consider the
// version of the document operation. If operations INDEX, UPDATE, or DELETE are supported
// in the future, this function needs to be redesigned.
func divideBulk(index string, reader io.Reader) (map[string][]protocol.Document, error) {
	documents := make(map[string][]protocol.Document)
	sc := bufio.NewScanner(reader)
	buf := make([]byte, maxBytesOfLine)
	sc.Buffer(buf, maxBytesOfLine)
	documentLine := false
	var lastMeta *protocol.BulkMeta
	for sc.Scan() {
		bytes := sc.Bytes()
		if len(bytes) == 0 {
			// skip blank lines
			continue
		}
		if documentLine {
			document := protocol.Document{}
			if err := json.Unmarshal(bytes, &document); err != nil {
				return nil, &errs.InvalidBulkError{Message: sc.Text()}
			}
			if _, found := document[consts.IDField]; !found {
				document[consts.IDField] = lastMeta.ID
			}
			if lastMeta.Index == "" {
				lastMeta.Index = index
			}
			documents[lastMeta.Index] = append(documents[lastMeta.Index], document)
			documentLine = false
		} else {
			action := protocol.BulkAction{}
			if err := json.Unmarshal(bytes, &action); err != nil {
				return nil, &errs.InvalidBulkError{Message: sc.Text()}
			}
			for actionName, actionMeta := range action {
				if !strings.EqualFold("create", actionName) {
					return nil, &errs.InvalidBulkError{Message: sc.Text()}
				}
				lastMeta = actionMeta
			}
			documentLine = true
		}
	}
	return documents, nil
}

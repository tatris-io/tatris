// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"
	"github.com/tatris-io/tatris/internal/meta"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
)

var metaStore storage.MetaStore

func init() {
	metaStore, _ = storage.Open()
}

func Create(idx *meta.Index) error {
	if json, err := json.Marshal(idx); err != nil {
		return err
	} else {
		return metaStore.Set(fillKey(idx.Name), json)
	}
}

func Get(idxName string) (*meta.Index, error) {
	if b, err := metaStore.Get(fillKey(idxName)); err != nil {
		return nil, err
	} else if b == nil {
		return nil, nil
	} else {
		idx := new(meta.Index)
		if err := json.Unmarshal(b, idx); err != nil {
			return nil, err
		} else {
			return idx, nil
		}
	}
}

func fillKey(name string) string {
	return "/index/" + name
}

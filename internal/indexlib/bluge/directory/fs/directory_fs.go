// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package fs is just a simple encapsulation of index.NewFileSystemDirectory for logging time cost.
package fs

import (
	"fmt"
	"io"

	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/tatris-io/tatris/internal/common/utils"
)

type FsDirectory struct {
	path string
	dir  *index.FileSystemDirectory
}

func NewFsDirectory(path string) *FsDirectory {
	return &FsDirectory{
		path: path,
		dir:  index.NewFileSystemDirectory(path),
	}
}

func (d *FsDirectory) Setup(readOnly bool) error {
	defer utils.Timerf(
		"[directory] method:setup, type:fs, path:%s, readOnly:%t",
		d.path,
		readOnly,
	)()
	return d.dir.Setup(readOnly)
}

func (d *FsDirectory) List(kind string) ([]uint64, error) {
	defer utils.Timerf(
		"[directory] method:list, type:fs, path:%s, kind:%s",
		d.path,
		kind,
	)()
	return d.dir.List(kind)
}

func (d *FsDirectory) Persist(
	kind string,
	id uint64,
	w index.WriterTo,
	closeCh chan struct{},
) error {
	defer utils.Timerf(
		"[directory] method:persist, type:fs, path:%s, filename:%s",
		d.path,
		d.fileName(kind, id),
	)()
	return d.dir.Persist(kind, id, w, closeCh)
}

func (d *FsDirectory) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	defer utils.Timerf(
		"[directory] method:load, type:fs, path:%s, filename:%s",
		d.path,
		d.fileName(kind, id),
	)()
	return d.dir.Load(kind, id)
}

func (d *FsDirectory) Remove(kind string, id uint64) error {
	defer utils.Timerf(
		"[directory] method:remove, type:fs, path:%s, filename:%s",
		d.path,
		d.fileName(kind, id),
	)()
	return d.dir.Remove(kind, id)
}

func (d *FsDirectory) Lock() error {
	return d.dir.Lock()
}

func (d *FsDirectory) Unlock() error {
	return d.dir.Unlock()
}

func (d *FsDirectory) Stats() (numFilesOnDisk, numBytesUsedDisk uint64) {
	defer utils.Timerf(
		"[directory] method:stats, type:fs, path:%s",
		d.path,
	)()
	return d.dir.Stats()
}

func (d *FsDirectory) Sync() error {
	defer utils.Timerf(
		"[directory] method:sync, type:fs, path:%s",
		d.path,
	)()
	return d.dir.Sync()
}

func (d *FsDirectory) fileName(kind string, id uint64) string {
	return fmt.Sprintf("%012x", id) + kind
}

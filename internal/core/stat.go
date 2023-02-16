// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

// Stat records the statistics of an index split
type Stat struct {
	MinTime int64
	MaxTime int64
	DocNum  int64
}

type ShardStat struct {
	Stat
	// WalIndex is the last consumed wal index
	WalIndex uint64
}

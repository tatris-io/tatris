package indexlib

const (
	TimestampField = "@timestamp"
	IDField        = "_id"
	SourceField    = "_source"
	IndexField     = "_index"
	TypeField      = "_type"

	BlugeIndexLibType = "bluge"
	DefaultDataPath   = "./data"
	FSStorageType     = "fs"
)

type BaseConfig struct {
	StorageType  string
	DataPath     string
	Index        string
	IndexLibType string
}

func NewBaseConfig(config *BaseConfig) *BaseConfig {
	if config.DataPath == "" {
		config.DataPath = DefaultDataPath
	}
	if config.StorageType == "" {
		config.StorageType = FSStorageType
	}
	if config.IndexLibType == "" {
		config.IndexLibType = BlugeIndexLibType
	}
	return config
}

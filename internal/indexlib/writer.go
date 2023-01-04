package indexlib

type Writer interface {
	OpenWriter() error
	Insert(docID string, doc map[string]interface{}) error
	Batch(docs map[string]map[string]interface{}) error
	Close()
}

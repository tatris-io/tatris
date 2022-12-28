package storage

type MetaStore interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
	Delete(string) error
}

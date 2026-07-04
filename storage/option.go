package storage

// StorageOption is retained for backwards compatibility with callers that
// build options from the two sub-package option structs. New code should
// use NewStorage directly with concrete Storage implementations.
//
// It is intentionally decoupled from the s3store/fsstore types: the
// conversion happens in internal/cliutil, which is the only place that
// imports both sub-packages. This keeps the storage package free of
// import cycles.
type StorageOption struct {
	remote Store
	local  Store
}

// NewStorageOption wraps already-constructed remote/local Store backends
// into a StorageOption. The constructor lives here so callers can pass the
// aggregate around without depending on the concrete types.
func NewStorageOption(remote, local Store) *StorageOption {
	return &StorageOption{remote: remote, local: local}
}

// Build assembles the aggregate Storage from the option.
func (o *StorageOption) Build() *Storage {
	return NewStorage(o.remote, o.local)
}

package storage

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	fsstore "github.com/LinPr/s6cmd/storage/fs"
	s3store "github.com/LinPr/s6cmd/storage/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// // Storage is an interface for storage operations that is common
// // to local filesystem and remote object storage.
// type Storage interface {
// 	// Stat returns the Object structure describing object. If src is not
// 	// found, ErrGivenObjectNotFound is returned.
// 	Stat(ctx context.Context, target StorageURL) (*Object, error)

// 	// List the objects and directories/prefixes in the src.
// 	List(ctx context.Context, target StorageURL, followSymlinks bool) <-chan *Object

// 	// Delete deletes the given src.
// 	Delete(ctx context.Context, target StorageURL) error

// 	// MultiDelete deletes all items returned from given urls in batches.
// 	MultiDelete(ctx context.Context, urls <-chan StorageURL) <-chan *Object

// 	// Copy src to dst, optionally setting the given metadata. Src and dst
// 	// arguments are of the same type. If src is a remote type, server side
// 	// copying will be used.
// 	Copy(ctx context.Context, src, dst StorageURL, metadata Metadata) error
// }

type Metadata struct {
	ACL                string
	CacheControl       string
	Expires            string
	StorageClass       string
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	EncryptionMethod   string
	EncryptionKeyID    string

	UserDefined map[string]string

	// MetadataDirective is used to specify whether the metadata is copied from
	// the source object or replaced with metadata provided when copying S3
	// objects. If MetadataDirective is not set, it defaults to "COPY".
	Directive string
}

// ObjectType is the type of Object.
type ObjectType struct {
	mode os.FileMode
}

// String returns the string representation of ObjectType.
func (o ObjectType) String() string {
	switch mode := o.mode; {
	case mode.IsRegular():
		return "file"
	case mode.IsDir():
		return "directory"
	case mode&os.ModeSymlink != 0:
		return "symlink"
	}
	return ""
}

// MarshalJSON returns the stringer of ObjectType as a marshalled json.
func (o ObjectType) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.String())
}

// IsDir checks if the object is a directory.
func (o ObjectType) IsDir() bool {
	return o.mode.IsDir()
}

// IsSymlink checks if the object is a symbolic link.
func (o ObjectType) IsSymlink() bool {
	return o.mode&os.ModeSymlink != 0
}

// IsRegular checks if the object is a regular file.
func (o ObjectType) IsRegular() bool {
	return o.mode.IsRegular()
}

// StorageClass represents the storage used to store an object.
type StorageClass string

// Object is a generic type which contains metadata for storage items.
type Object struct {
	StorageURL   *StorageURL  `json:"key,omitempty"`
	Etag         string       `json:"etag,omitempty"`
	ModTime      *time.Time   `json:"last_modified,omitempty"`
	Type         ObjectType   `json:"type,omitempty"`
	Size         int64        `json:"size,omitempty"`
	StorageClass StorageClass `json:"storage_class,omitempty"`
	Err          error        `json:"error,omitempty"`
	retryID      string

	// the VersionID field exist only for JSON Marshall, it must not be used for
	// any other purpose. URL.VersionID must be used instead.
	VersionID string `json:"version_id,omitempty"`
}

type Storage struct {
	remote *s3store.S3Store
	local  *fsstore.FileStore
}

func NewStorage(ctx context.Context) (*Storage, error) {
	s3client, err := s3store.NewS3Client(ctx)
	if err != nil {
		return nil, err
	}
	fs := fsstore.NewFileStore()

	return &Storage{
		remote: s3client,
		local:  fs,
	}, nil
}

func (s *Storage) DownloadFile(ctx context.Context, bucketName string, objectKey string, localFile string) error {

	result, err := s.remote.GetObject(ctx, bucketName, objectKey)
	if err != nil {
		return err
	}
	defer result.Body.Close()

	file, err := s.local.Create(localFile)
	if err != nil {
		log.Printf("Couldn't create file %v. err: %v\n", localFile, err)
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, result.Body)
	return err
}

func (s *Storage) UploadFile(ctx context.Context, fileName string, bucketName string, objectKey string) (*s3.PutObjectOutput, error) {

	file, err := s.local.Create(fileName)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	return s.remote.PutObject(ctx, file, bucketName, objectKey)

}

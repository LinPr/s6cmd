package s3store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/LinPr/s6cmd/internal/errorpkg"
	"github.com/LinPr/s6cmd/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// deleteObjectsMax is the S3 limit for a single DeleteObjects request.
const deleteObjectsMax = 1000

// Stat retrieves metadata for the given S3 object. If the object does not
// exist it returns errorpkg.ErrGivenObjectNotFound.
func (s *S3Store) Stat(ctx context.Context, url *storage.StorageURL) (*storage.Object, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.requestPayer(),
	}
	if url.VersionID != "" {
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.client.HeadObject(ctx, input)
	if err != nil {
		return nil, statObjectNotFound(url, err)
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)
	obj := &storage.Object{
		StorageURL: url,
		Etag:       trimEtag(etag),
		ModTime:    &mod,
		Size:       aws.ToInt64(output.ContentLength),
	}
	// When NoSuchUpload retry is enabled, Stat is called from Put's retry
	// loop to compare the retry-id metadata against the value sent with
	// the upload. Populate the unexported retryID field via the accessor
	// so callers outside the s3store package can read it.
	if s.noSuchUploadRetryCount > 0 && len(output.Metadata) > 0 {
		if v, ok := output.Metadata[metadataKeyRetryID]; ok {
			obj.SetRetryID(v)
		}
	}
	return obj, nil
}

// List is a non-blocking S3 list operation which paginates and filters S3
// keys. If no object is found or an error is encountered during listing, it
// is sent to the returned channel as an Object with Err set.
func (s *S3Store) List(ctx context.Context, url *storage.StorageURL, _ bool) <-chan *storage.Object {
	if url.VersionID != "" || url.AllVersions {
		return s.listObjectVersions(ctx, url)
	}
	if s.useListObjectsV1 {
		return s.listObjects(ctx, url)
	}
	return s.listObjectsV2(ctx, url)
}

// listObjectsV2 paginates ListObjectsV2 and emits Objects matching url.
func (s *S3Store) listObjectsV2(ctx context.Context, url *storage.StorageURL) <-chan *storage.Object {
	objCh := make(chan *storage.Object)

	go func() {
		defer close(objCh)

		listInput := &s3.ListObjectsV2Input{
			Bucket:       aws.String(url.Bucket),
			Prefix:       aws.String(url.Prefix),
			RequestPayer: s.requestPayer(),
		}
		if url.Delimiter != "" {
			listInput.Delimiter = aws.String(url.Delimiter)
		}

		paginator := s3.NewListObjectsV2Paginator(s.client, listInput)
		objectFound := false

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				sendObject(ctx, &storage.Object{Err: err}, objCh)
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}
				newURL := url.Clone()
				newURL.Path = prefix
				objectFound = true
				sendObject(ctx, &storage.Object{
					StorageURL: newURL,
					Type:       storage.NewObjectType(os.ModeDir | 0o755), // directory bit
				}, objCh)
			}

			for _, obj := range page.Contents {
				key := aws.ToString(obj.Key)
				if !url.Match(key) {
					continue
				}
				objectFound = true
				mod := aws.ToTime(obj.LastModified)
				newURL := url.Clone()
				newURL.Path = key
				sendObject(ctx, &storage.Object{
					StorageURL:   newURL,
					Etag:         trimEtag(aws.ToString(obj.ETag)),
					ModTime:      &mod,
					Size:         aws.ToInt64(obj.Size),
					StorageClass: storage.StorageClass(string(obj.StorageClass)),
				}, objCh)
			}
		}

		if !objectFound {
			sendObject(ctx, &storage.Object{Err: errorpkg.ErrNoObjectFound}, objCh)
		}
	}()

	return objCh
}

// listObjects is the legacy ListObjects (V1) variant, used for
// S3-compatible services that do not implement V2. Unlike V2 there is no
// handwritten paginator in the SDK, so we paginate manually via the
// Marker field.
func (s *S3Store) listObjects(ctx context.Context, url *storage.StorageURL) <-chan *storage.Object {
	objCh := make(chan *storage.Object)

	go func() {
		defer close(objCh)

		listInput := &s3.ListObjectsInput{
			Bucket:       aws.String(url.Bucket),
			Prefix:       aws.String(url.Prefix),
			RequestPayer: s.requestPayer(),
		}
		if url.Delimiter != "" {
			listInput.Delimiter = aws.String(url.Delimiter)
		}

		objectFound := false
		// prevMarker remembers the marker of the previous page so a
		// misbehaving server that keeps returning IsTruncated=true without
		// advancing the marker cannot loop this goroutine forever.
		prevMarker := ""
		for {
			page, err := s.client.ListObjects(ctx, listInput)
			if err != nil {
				sendObject(ctx, &storage.Object{Err: err}, objCh)
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}
				newURL := url.Clone()
				newURL.Path = prefix
				objectFound = true
				sendObject(ctx, &storage.Object{
					StorageURL: newURL,
					Type:       storage.NewObjectType(os.ModeDir | 0o755),
				}, objCh)
			}

			for _, obj := range page.Contents {
				key := aws.ToString(obj.Key)
				if !url.Match(key) {
					continue
				}
				objectFound = true
				mod := aws.ToTime(obj.LastModified)
				newURL := url.Clone()
				newURL.Path = key
				sendObject(ctx, &storage.Object{
					StorageURL:   newURL,
					Etag:         trimEtag(aws.ToString(obj.ETag)),
					ModTime:      &mod,
					Size:         aws.ToInt64(obj.Size),
					StorageClass: storage.StorageClass(string(obj.StorageClass)),
				}, objCh)
			}

			if !aws.ToBool(page.IsTruncated) {
				break
			}
			var nextMarker string
			switch {
			case page.NextMarker != nil && *page.NextMarker != "":
				nextMarker = *page.NextMarker
			case len(page.Contents) > 0:
				// S3 only returns NextMarker when Delimiter is set. Per
				// the ListObjects contract, the last returned key is the
				// marker for the next page in that case.
				nextMarker = aws.ToString(page.Contents[len(page.Contents)-1].Key)
			default:
				// An empty Contents page with IsTruncated set has no
				// usable marker; break rather than loop forever.
				nextMarker = ""
			}
			// Keys are listed in ascending order, so a valid next marker is
			// always strictly greater than the previous one. A server that
			// reports IsTruncated=true without advancing the marker (e.g.
			// re-emitting the same page) would otherwise loop forever.
			if nextMarker == "" || nextMarker <= prevMarker {
				sendObject(ctx, &storage.Object{
					Err: fmt.Errorf("listObjects: server returned a truncated page with a non-advancing marker %q; aborting listing to avoid an infinite loop", nextMarker),
				}, objCh)
				return
			}
			prevMarker = nextMarker
			listInput.Marker = aws.String(nextMarker)
		}

		if !objectFound {
			sendObject(ctx, &storage.Object{Err: errorpkg.ErrNoObjectFound}, objCh)
		}
	}()

	return objCh
}

// listObjectVersions paginates ListObjectVersions and emits Objects.
func (s *S3Store) listObjectVersions(ctx context.Context, url *storage.StorageURL) <-chan *storage.Object {
	objCh := make(chan *storage.Object)

	go func() {
		defer close(objCh)

		listInput := &s3.ListObjectVersionsInput{
			Bucket:       aws.String(url.Bucket),
			Prefix:       aws.String(url.Prefix),
			RequestPayer: s.requestPayer(),
		}
		if url.Delimiter != "" {
			listInput.Delimiter = aws.String(url.Delimiter)
		}

		paginator := s3.NewListObjectVersionsPaginator(s.client, listInput)
		objectFound := false

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				sendObject(ctx, &storage.Object{Err: err}, objCh)
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := aws.ToString(c.Prefix)
				if !url.Match(prefix) {
					continue
				}
				newURL := url.Clone()
				newURL.Path = prefix
				objectFound = true
				sendObject(ctx, &storage.Object{
					StorageURL: newURL,
					Type:       storage.NewObjectType(os.ModeDir | 0o755),
				}, objCh)
			}

			for _, obj := range page.Versions {
				key := aws.ToString(obj.Key)
				if !url.Match(key) {
					continue
				}
				objectFound = true
				mod := aws.ToTime(obj.LastModified)
				newURL := url.Clone()
				newURL.Path = key
				newURL.VersionID = aws.ToString(obj.VersionId)
				sendObject(ctx, &storage.Object{
					StorageURL:   newURL,
					Etag:         trimEtag(aws.ToString(obj.ETag)),
					ModTime:      &mod,
					Size:         aws.ToInt64(obj.Size),
					StorageClass: storage.StorageClass(string(obj.StorageClass)),
					VersionID:    aws.ToString(obj.VersionId),
				}, objCh)
			}

			// Delete markers are first-class entries in a versioned bucket:
			// without deleting them a bucket can never be fully purged
			// (DeleteBucket keeps failing with BucketNotEmpty). Emit them as
			// Key+VersionId objects flagged IsDeleteMarker so callers can
			// render them distinctly.
			for _, marker := range page.DeleteMarkers {
				key := aws.ToString(marker.Key)
				if !url.Match(key) {
					continue
				}
				objectFound = true
				mod := aws.ToTime(marker.LastModified)
				newURL := url.Clone()
				newURL.Path = key
				newURL.VersionID = aws.ToString(marker.VersionId)
				sendObject(ctx, &storage.Object{
					StorageURL:     newURL,
					ModTime:        &mod,
					VersionID:      aws.ToString(marker.VersionId),
					IsDeleteMarker: true,
				}, objCh)
			}
		}

		if !objectFound {
			sendObject(ctx, &storage.Object{Err: errorpkg.ErrNoObjectFound}, objCh)
		}
	}()

	return objCh
}

// Copy performs a server-side CopyObject from src to dst, applying the given
// metadata. The metadata directive defaults to COPY when unset.
func (s *S3Store) Copy(ctx context.Context, src, dst *storage.StorageURL, metadata storage.Metadata) error {
	if s.dryRun {
		return nil
	}

	input := &s3.CopyObjectInput{
		Bucket:       aws.String(dst.Bucket),
		CopySource:   aws.String(src.EscapedPath()),
		Key:          aws.String(dst.Path),
		RequestPayer: s.requestPayer(),
	}

	if metadata.Directive != "" {
		input.MetadataDirective = types.MetadataDirective(metadata.Directive)
	}
	if metadata.ACL != "" {
		input.ACL = types.ObjectCannedACL(metadata.ACL)
	}
	if metadata.CacheControl != "" {
		input.CacheControl = aws.String(metadata.CacheControl)
	}
	if metadata.ContentType != "" {
		input.ContentType = aws.String(metadata.ContentType)
	}
	if metadata.ContentEncoding != "" {
		input.ContentEncoding = aws.String(metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		input.ContentDisposition = aws.String(metadata.ContentDisposition)
	}
	if metadata.Expires != "" {
		t, err := time.Parse(time.RFC3339, metadata.Expires)
		if err != nil {
			return fmt.Errorf("parse expires: %w", err)
		}
		input.Expires = aws.Time(t)
	}
	if metadata.StorageClass != "" {
		input.StorageClass = types.StorageClass(metadata.StorageClass)
	}
	if metadata.EncryptionMethod != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(metadata.EncryptionMethod)
		if metadata.EncryptionKeyID != "" {
			input.SSEKMSKeyId = aws.String(metadata.EncryptionKeyID)
		}
	}
	if len(metadata.UserDefined) > 0 {
		input.Metadata = metadata.UserDefined
	}

	_, err := s.client.CopyObject(ctx, input)
	return err
}

// Delete deletes a single S3 object. When the URL carries a VersionID the
// specific version is deleted permanently; without it a versioned bucket
// just records a delete marker.
func (s *S3Store) Delete(ctx context.Context, url *storage.StorageURL) error {
	if s.dryRun {
		return nil
	}
	input := &s3.DeleteObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.requestPayer(),
	}
	if url.VersionID != "" {
		input.VersionId = aws.String(url.VersionID)
	}
	_, err := s.client.DeleteObject(ctx, input)
	return err
}

// chunk groups ObjectIdentifiers for batched DeleteObjects calls.
type chunk struct {
	Bucket string
	Keys   []types.ObjectIdentifier
}

// calculateChunks reads URLs from urlch and emits delete-sized chunks. A
// chunk never spans buckets: when the incoming bucket changes, the current
// chunk is flushed so its keys are deleted against the bucket they belong
// to.
func calculateChunks(urlch <-chan *storage.StorageURL) <-chan chunk {
	chunkch := make(chan chunk)
	go func() {
		defer close(chunkch)

		var keys []types.ObjectIdentifier
		var bucket string
		for u := range urlch {
			if u.Bucket != bucket && len(keys) > 0 {
				chunkch <- chunk{Bucket: bucket, Keys: keys}
				keys = nil
			}
			bucket = u.Bucket
			objid := types.ObjectIdentifier{Key: aws.String(u.Path)}
			if u.VersionID != "" {
				objid.VersionId = aws.String(u.VersionID)
			}
			keys = append(keys, objid)
			if len(keys) == deleteObjectsMax {
				chunkch <- chunk{Bucket: bucket, Keys: keys}
				keys = nil
			}
		}
		if len(keys) > 0 {
			chunkch <- chunk{Bucket: bucket, Keys: keys}
		}
	}()
	return chunkch
}

// MultiDelete deletes every URL read from urlch in batches of 1000 and
// returns a channel of Objects carrying per-URL success/error status.
func (s *S3Store) MultiDelete(ctx context.Context, urlch <-chan *storage.StorageURL) <-chan *storage.Object {
	resultch := make(chan *storage.Object)

	go func() {
		defer close(resultch)

		for c := range calculateChunks(urlch) {
			if s.dryRun {
				for _, k := range c.Keys {
					key := aws.ToString(k.Key)
					u, _ := storage.NewStorageURL("s3://" + c.Bucket + "/" + key)
					if u != nil {
						u.VersionID = aws.ToString(k.VersionId)
					}
					resultch <- &storage.Object{StorageURL: u}
				}
				continue
			}

			output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket:       aws.String(c.Bucket),
				Delete:       &types.Delete{Objects: c.Keys, Quiet: aws.Bool(true)},
				RequestPayer: s.requestPayer(),
			}, withContentMD5)
			if err != nil {
				// Emit the error but keep consuming the remaining chunks:
				// returning here would leave calculateChunks (and the
				// upstream URL producer) blocked on a send forever.
				resultch <- &storage.Object{Err: err}
				continue
			}
			// Quiet=true suppresses output.Deleted, so successes are derived
			// as the chunk's keys minus the reported errors.
			failed := make(map[string]struct{}, len(output.Errors))
			for _, derr := range output.Errors {
				failed[aws.ToString(derr.Key)+"\x00"+aws.ToString(derr.VersionId)] = struct{}{}
				u, _ := storage.NewStorageURL("s3://" + c.Bucket + "/" + aws.ToString(derr.Key))
				if u != nil {
					u.VersionID = aws.ToString(derr.VersionId)
				}
				resultch <- &storage.Object{
					StorageURL: u,
					Err:        fmt.Errorf("%s: %s", aws.ToString(derr.Code), aws.ToString(derr.Message)),
				}
			}
			for _, k := range c.Keys {
				if _, ok := failed[aws.ToString(k.Key)+"\x00"+aws.ToString(k.VersionId)]; ok {
					continue
				}
				u, _ := storage.NewStorageURL("s3://" + c.Bucket + "/" + aws.ToString(k.Key))
				if u != nil {
					u.VersionID = aws.ToString(k.VersionId)
				}
				resultch <- &storage.Object{StorageURL: u}
			}
		}
	}()

	return resultch
}

// --- Multipart download/upload ---

// Get downloads the object at from into the io.WriterAt to using the
// multipart downloader with the requested concurrency and part size.
func (s *S3Store) Get(ctx context.Context, from *storage.StorageURL, to io.WriterAt, concurrency int, partSize int64) (int64, error) {
	if s.dryRun {
		return 0, nil
	}
	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.requestPayer(),
	}
	if from.VersionID != "" {
		input.VersionId = aws.String(from.VersionID)
	}
	return s.downloader.Download(ctx, to, input, func(d *manager.Downloader) {
		if partSize > 0 {
			d.PartSize = partSize
		}
		if concurrency > 0 {
			d.Concurrency = concurrency
		}
	})
}

// Put uploads reader to the URL using the multipart uploader with the
// requested concurrency and part size, applying the given metadata.
func (s *S3Store) Put(ctx context.Context, reader io.Reader, to *storage.StorageURL, metadata storage.Metadata, concurrency int, partSize int64) error {
	if s.dryRun {
		return nil
	}

	contentType := metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(to.Bucket),
		Key:          aws.String(to.Path),
		Body:         reader,
		ContentType:  aws.String(contentType),
		RequestPayer: s.requestPayer(),
	}
	if metadata.ACL != "" {
		input.ACL = types.ObjectCannedACL(metadata.ACL)
	}
	if metadata.CacheControl != "" {
		input.CacheControl = aws.String(metadata.CacheControl)
	}
	if metadata.ContentEncoding != "" {
		input.ContentEncoding = aws.String(metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		input.ContentDisposition = aws.String(metadata.ContentDisposition)
	}
	if metadata.Expires != "" {
		t, err := time.Parse(time.RFC3339, metadata.Expires)
		if err != nil {
			return fmt.Errorf("parse expires: %w", err)
		}
		input.Expires = aws.Time(t)
	}
	if metadata.StorageClass != "" {
		input.StorageClass = types.StorageClass(metadata.StorageClass)
	}
	if metadata.EncryptionMethod != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(metadata.EncryptionMethod)
		if metadata.EncryptionKeyID != "" {
			input.SSEKMSKeyId = aws.String(metadata.EncryptionKeyID)
		}
	}
	if len(metadata.UserDefined) > 0 {
		input.Metadata = metadata.UserDefined
	}

	// NoSuchUpload retry: stamp a per-upload retry id so that, if the
	// uploader returns NoSuchUpload, we can Stat the target and tell
	// whether a previous attempt actually wrote the object. The retry
	// loop lives in retryOnNoSuchUpload below.
	if s.noSuchUploadRetryCount > 0 {
		if input.Metadata == nil {
			input.Metadata = map[string]string{}
		}
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	_, err := s.uploader.Upload(ctx, input, func(u *manager.Uploader) {
		if partSize > 0 {
			u.PartSize = partSize
		}
		if concurrency > 0 {
			u.Concurrency = concurrency
		}
	})
	if err != nil && s.noSuchUploadRetryCount > 0 && errHasCode(err, "NoSuchUpload") {
		return s.retryOnNoSuchUpload(ctx, to, input, err, func(u *manager.Uploader) {
			if partSize > 0 {
				u.PartSize = partSize
			}
			if concurrency > 0 {
				u.Concurrency = concurrency
			}
		})
	}
	return err
}

// retryOnNoSuchUpload handles NoSuchUpload by checking whether a previous
// attempt actually succeeded. When the uploader returns NoSuchUpload, the
// target object is Statted; if its s6cmd-upload-retry-id metadata matches
// the value sent with the upload, the upload is treated as successful (a
// previous attempt completed despite the error). Otherwise the upload is
// retried up to noSuchUploadRetryCount times.
//
// The retry id is read from input.Metadata, which the caller populated
// before the first attempt. The same id is reused for every retry so a
// successful retry can be detected by a subsequent Stat.
//
// The failed attempt consumed input.Body, so a retry must rewind it to the
// start before re-uploading. When the body is not seekable (stdin/pipe) a
// retry would upload truncated data; in that case the original error is
// returned with a message explaining why no retry happened.
func (s *S3Store) retryOnNoSuchUpload(ctx context.Context, to *storage.StorageURL, input *s3.PutObjectInput, err error, opts ...func(*manager.Uploader)) error {
	expectedRetryID := input.Metadata[metadataKeyRetryID]
	seeker, seekable := input.Body.(io.Seeker)

	attempts := 0
	for errHasCode(err, "NoSuchUpload") && attempts < s.noSuchUploadRetryCount {
		attempts++
		obj, sErr := s.Stat(ctx, to)
		if sErr == nil && obj != nil && obj.RetryID() == expectedRetryID && expectedRetryID != "" {
			return nil
		}
		if !seekable {
			return fmt.Errorf("RetryOnNoSuchUpload: cannot retry upload of %q: request body is not seekable (e.g. stdin/pipe): %w", to.String(), err)
		}
		if _, seekErr := seeker.Seek(0, io.SeekStart); seekErr != nil {
			return fmt.Errorf("RetryOnNoSuchUpload: rewind request body for retry: %v: %w", seekErr, err)
		}
		_, err = s.uploader.Upload(ctx, input, opts...)
	}

	if err != nil && errHasCode(err, "NoSuchUpload") && s.noSuchUploadRetryCount > 0 {
		return fmt.Errorf("RetryOnNoSuchUpload: %d attempts to retry resulted in NoSuchUpload: %w", attempts, err)
	}
	return err
}

// errHasCode reports whether err is (or wraps) an S3 error with the given
// code. v2 surfaces NoSuchUpload as the typed *types.NoSuchUpload, but
// S3-compatible services may return a generic smithy.APIError whose
// ErrorCode() string is the only carrier; both are covered.
func errHasCode(err error, code string) bool {
	if err == nil || code == "" {
		return false
	}
	// Typed v2 error: NoSuchUpload is the only code we currently match
	// this way, but the helper is generic so future callers can pass
	// other codes without changing the function.
	switch code {
	case "NoSuchUpload":
		var nse *types.NoSuchUpload
		if errors.As(err, &nse) {
			return true
		}
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}
	return false
}

// generateRetryID returns a random hex string used as the per-upload retry
// id. crypto/rand is used so the id is not predictable; hex encoding keeps
// it safe to put in object metadata (no quoting/escaping).
func generateRetryID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read should never fail on Linux; if it does, fall back to
		// a timestamp-derived value so uploads still proceed. The retry
		// path is best-effort: a collision here only matters if two
		// concurrent uploads of the same key race, which the uploader
		// already serializes via the multipart upload id.
		return fmt.Sprintf("retry-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Read returns an io.ReadCloser for the object body. Caller must close it.
func (s *S3Store) Read(ctx context.Context, src *storage.StorageURL) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(src.Bucket),
		Key:          aws.String(src.Path),
		RequestPayer: s.requestPayer(),
	}
	if src.VersionID != "" {
		input.VersionId = aws.String(src.VersionID)
	}
	resp, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Presign returns a presigned GET URL for the object valid for expire. The
// URL's VersionID (when set) and the store's RequestPayer are baked into the
// signed request.
func (s *S3Store) Presign(ctx context.Context, from *storage.StorageURL, expire time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(from.Bucket),
		Key:          aws.String(from.Path),
		RequestPayer: s.requestPayer(),
	}
	if from.VersionID != "" {
		input.VersionId = aws.String(from.VersionID)
	}
	req, err := s.presigner.PresignGetObject(ctx, input, func(o *s3.PresignOptions) {
		o.Expires = expire
	})
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// Select runs an S3 Select query against the object at url and streams each
// decoded record (a json.RawMessage) onto resultCh. The implementation
// follows the v2 EventStream model: SelectObjectContent returns a
// *SelectObjectContentOutput whose GetStream() returns an event stream
// consumed via its Events() channel. The stream emits typed union members
// (*types.SelectObjectContentEventStreamMemberRecords, ...Stats, ...End,
// ...Cont, ...Progress); only Records carry user data, which is forwarded
// verbatim to resultCh.
//
// v2 (unlike v1) already buffers the events onto a channel, so we don't
// need an io.Pipe — we range the channel, append each Records payload to a
// running buffer, and emit a json.RawMessage per line. S3 Select's JSON
// output mode produces one JSON object per line; CSV output produces
// non-JSON rows, which are still pushed through as RawMessage bytes so the
// caller can decide how to render them.
func (s *S3Store) Select(ctx context.Context, url *storage.StorageURL, query *storage.SelectQuery, resultCh chan<- json.RawMessage) error {
	if s.dryRun {
		return nil
	}
	if query == nil {
		return errors.New("select: query must be non-nil")
	}
	if query.Expression == "" {
		return errors.New("select: query expression must be non-empty")
	}

	inputSer, err := buildInputSerialization(query)
	if err != nil {
		return err
	}
	outputSer, err := buildOutputSerialization(query)
	if err != nil {
		return err
	}

	expressionType := types.ExpressionTypeSql
	if query.ExpressionType != "" {
		expressionType = types.ExpressionType(query.ExpressionType)
	}

	input := &s3.SelectObjectContentInput{
		Bucket:              aws.String(url.Bucket),
		Key:                 aws.String(url.Path),
		ExpressionType:      expressionType,
		Expression:          aws.String(query.Expression),
		InputSerialization:  inputSer,
		OutputSerialization: outputSer,
	}
	// RequestPayer is not a modelled field on SelectObjectContentInput in
	// v2 (S3 Select does not support requester-pays), so we only forward
	// the SSE-C fields when a version is requested. SSE-C is not wired up
	// here; it would require additional flags on the select command.
	_ = url.VersionID

	resp, err := s.client.SelectObjectContent(ctx, input)
	if err != nil {
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	// S3 Select may split a single logical record across multiple Records
	// events, so we accumulate payload bytes and split on newlines. This
	// yields one json.RawMessage per line regardless of whether the
	// underlying output is JSON-lines or CSV.
	var buf []byte
	for event := range stream.Events() {
		// Respect ctx cancellation: if the consumer (e.g. stdout pipe
		// broken) cancelled the context, stop reading and return.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		switch e := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			buf = append(buf, e.Value.Payload...)
			// Flush complete lines so memory does not grow unbounded for
			// large result sets.
			for {
				idx := bytesIndexByte(buf, '\n')
				if idx < 0 {
					break
				}
				line := buf[:idx]
				buf = buf[idx+1:]
				if len(line) == 0 {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resultCh <- append(json.RawMessage(nil), line...):
				}
			}
		case *types.SelectObjectContentEventStreamMemberStats:
			// Stats events carry processing statistics; we ignore them so
			// the result stream contains only user records.
		case *types.SelectObjectContentEventStreamMemberProgress:
			// Progress events are periodic heartbeat events; ignored.
		case *types.SelectObjectContentEventStreamMemberCont:
			// Continuation events are S3's signal that more data is coming;
			// no action needed.
		case *types.SelectObjectContentEventStreamMemberEnd:
			// End event: the stream is logically complete; the for-range
			// will exit when the Events channel closes.
		}
	}
	if err := stream.Err(); err != nil {
		return err
	}
	// Flush any trailing line without a newline terminator.
	if len(buf) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultCh <- append(json.RawMessage(nil), buf...):
		}
	}
	return nil
}

// buildInputSerialization constructs the v2 InputSerialization for the
// given SelectQuery. The mapping is: json → JSONInput{Type}, csv →
// CSVInput{FieldDelimiter, FileHeaderInfo}, parquet → ParquetInput{}.
// Compression is applied only for json and csv.
func buildInputSerialization(q *storage.SelectQuery) (*types.InputSerialization, error) {
	s := &types.InputSerialization{}
	switch q.InputFormat {
	case "json":
		jsonType := types.JSONTypeLines
		if q.InputContentStructure == "document" {
			jsonType = types.JSONTypeDocument
		}
		s.JSON = &types.JSONInput{Type: jsonType}
		if q.CompressionType != "" {
			ct, err := parseCompressionType(q.CompressionType)
			if err != nil {
				return nil, err
			}
			s.CompressionType = ct
		}
	case "csv":
		csv := &types.CSVInput{}
		if q.InputContentStructure != "" {
			csv.FieldDelimiter = aws.String(q.InputContentStructure)
		} else {
			csv.FieldDelimiter = aws.String(",")
		}
		csv.FileHeaderInfo = parseFileHeaderInfo(q.FileHeaderInfo)
		s.CSV = csv
		if q.CompressionType != "" {
			ct, err := parseCompressionType(q.CompressionType)
			if err != nil {
				return nil, err
			}
			s.CompressionType = ct
		}
	case "parquet":
		s.Parquet = &types.ParquetInput{}
	case "":
		// Default to JSON when the caller did not specify a format.
		s.JSON = &types.JSONInput{Type: types.JSONTypeLines}
	default:
		return nil, fmt.Errorf("select: unsupported input format %q", q.InputFormat)
	}
	return s, nil
}

// buildOutputSerialization constructs the v2 OutputSerialization. When the
// user did not specify an output format, it defaults to the input format.
// CSV output reuses the input's delimiter when set.
func buildOutputSerialization(q *storage.SelectQuery) (*types.OutputSerialization, error) {
	format := q.OutputFormat
	if format == "" {
		format = q.InputFormat
	}
	switch format {
	case "json":
		return &types.OutputSerialization{JSON: &types.JSONOutput{}}, nil
	case "csv":
		delimiter := q.InputContentStructure
		if delimiter == "" {
			delimiter = ","
		}
		return &types.OutputSerialization{CSV: &types.CSVOutput{
			FieldDelimiter: aws.String(delimiter),
		}}, nil
	case "":
		return &types.OutputSerialization{JSON: &types.JSONOutput{}}, nil
	default:
		return nil, fmt.Errorf("select: unsupported output format %q", format)
	}
}

// parseCompressionType maps the user-facing string ("GZIP"/"BZIP2"/"NONE")
// to the v2 CompressionType enum. Empty/"NONE" yields CompressionTypeNone.
func parseCompressionType(s string) (types.CompressionType, error) {
	switch strings.ToUpper(s) {
	case "", "NONE":
		return types.CompressionTypeNone, nil
	case "GZIP":
		return types.CompressionTypeGzip, nil
	case "BZIP2":
		return types.CompressionTypeBzip2, nil
	}
	return "", fmt.Errorf("select: unsupported compression type %q", s)
}

// parseFileHeaderInfo maps the user-facing string ("USE"/"IGNORE"/"NONE")
// to the v2 FileHeaderInfo enum. Empty yields FileHeaderInfoNone.
func parseFileHeaderInfo(s string) types.FileHeaderInfo {
	switch strings.ToUpper(s) {
	case "USE":
		return types.FileHeaderInfoUse
	case "IGNORE":
		return types.FileHeaderInfoIgnore
	case "", "NONE":
		return types.FileHeaderInfoNone
	}
	// Unknown values default to NONE so a typo does not break the query.
	return types.FileHeaderInfoNone
}

// bytesIndexByte is a tiny wrapper so this file does not need to import
// bytes solely for one call.
func bytesIndexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

// --- HeadObject & legacy helpers ---

// HeadObject returns the generic Object and Metadata for the given URL.
func (s *S3Store) HeadObject(ctx context.Context, url *storage.StorageURL) (*storage.Object, *storage.Metadata, error) {
	input := &s3.HeadObjectInput{
		Bucket:       aws.String(url.Bucket),
		Key:          aws.String(url.Path),
		RequestPayer: s.requestPayer(),
	}
	if url.VersionID != "" {
		input.VersionId = aws.String(url.VersionID)
	}

	output, err := s.client.HeadObject(ctx, input)
	if err != nil {
		return nil, nil, statObjectNotFound(url, err)
	}

	etag := aws.ToString(output.ETag)
	mod := aws.ToTime(output.LastModified)
	obj := &storage.Object{
		StorageURL:   url,
		Etag:         trimEtag(etag),
		ModTime:      &mod,
		Size:         aws.ToInt64(output.ContentLength),
		StorageClass: storage.StorageClass(string(output.StorageClass)),
	}

	md := &storage.Metadata{
		ContentType:        aws.ToString(output.ContentType),
		ContentEncoding:    aws.ToString(output.ContentEncoding),
		ContentDisposition: aws.ToString(output.ContentDisposition),
		StorageClass:       string(output.StorageClass),
		EncryptionMethod:   string(output.ServerSideEncryption),
		EncryptionKeyID:    aws.ToString(output.SSEKMSKeyId),
	}
	if len(output.Metadata) > 0 {
		md.UserDefined = output.Metadata
	}
	return obj, md, nil
}

// HeadObjectOutput returns the raw SDK HeadObjectOutput for the given
// bucket/key. It is kept for backwards compatibility with cmd/stat and
// cmd/du which still read the raw v2 struct.
func (s *S3Store) HeadObjectOutput(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error) {
	return s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		RequestPayer: s.requestPayer(),
	})
}

// ListObjectKeysRecursive lists every object key under (bucket, prefix)
// recursively. It is the stringly-typed fallback used by existing cmd/*
// code paths.
func (s *S3Store) ListObjectKeysRecursive(ctx context.Context, bucket, prefix string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(prefix),
		RequestPayer: s.requestPayer(),
	})

	keys := make([]string, 0, 128)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

// ListObjectsWithPagination paginates ListObjectsV2 with the given delimiter
// (pass "" for recursive listing) and returns the raw types.Object and
// types.CommonPrefix slices. Kept for cmd/ls which prints the hierarchy
// itself.
//
// pageSize > 0 is forwarded as MaxKeys (the paginator uses it as its page
// limit); noPaginate stops after the first page so --no-paginate returns at
// most one page of results.
func (s *S3Store) ListObjectsWithPagination(ctx context.Context, bucket, key, delimiter string, pageSize int32, noPaginate bool) ([]types.Object, []types.CommonPrefix, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(key),
		RequestPayer: s.requestPayer(),
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if pageSize > 0 {
		input.MaxKeys = aws.Int32(pageSize)
	}
	paginator := s3.NewListObjectsV2Paginator(s.client, input)

	var objects []types.Object
	var prefixes []types.CommonPrefix
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, err
		}
		objects = append(objects, page.Contents...)
		prefixes = append(prefixes, page.CommonPrefixes...)
		if noPaginate {
			break
		}
	}
	return objects, prefixes, nil
}

// DeleteObjects deletes the given keys from the bucket in batched
// DeleteObjects calls of at most deleteObjectsMax keys each (S3 rejects
// bigger requests). Kept for the existing stringly-typed callers.
func (s *S3Store) DeleteObjects(ctx context.Context, bucketName string, objectKeys []string) error {
	if len(objectKeys) == 0 {
		return nil
	}
	if s.dryRun {
		return nil
	}
	var b strings.Builder
	for start := 0; start < len(objectKeys); start += deleteObjectsMax {
		end := start + deleteObjectsMax
		if end > len(objectKeys) {
			end = len(objectKeys)
		}
		objectIds := make([]types.ObjectIdentifier, 0, end-start)
		for _, key := range objectKeys[start:end] {
			objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
		}
		output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket:       aws.String(bucketName),
			Delete:       &types.Delete{Objects: objectIds, Quiet: aws.Bool(true)},
			RequestPayer: s.requestPayer(),
		}, withContentMD5)
		if err != nil {
			// A request-level failure must not drop the per-key errors
			// already collected from earlier batches; return both.
			var perKey error
			if b.Len() > 0 {
				perKey = errors.New(strings.TrimSuffix(b.String(), "; "))
			}
			return errors.Join(perKey, err)
		}
		for _, e := range output.Errors {
			fmt.Fprintf(&b, "%s: %s; ", aws.ToString(e.Key), aws.ToString(e.Message))
		}
	}
	if b.Len() > 0 {
		return errors.New(strings.TrimSuffix(b.String(), "; "))
	}
	return nil
}

// sendObject pushes obj onto ch unless ctx is cancelled.
func sendObject(ctx context.Context, obj *storage.Object, ch chan<- *storage.Object) {
	select {
	case <-ctx.Done():
	case ch <- obj:
	}
}

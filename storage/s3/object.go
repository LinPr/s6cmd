package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func (s3store *S3Store) UploadFile(ctx context.Context, bucketName string, objectKey string, fileName string) error {

	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Couldn't open file %v to upload. Here's why: %v\n", fileName, err)
	} else {
		defer file.Close()
		_, err = s3store.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
				log.Printf("Error while uploading object to %s. The object is too large.\n"+
					"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
					"or the multipart upload API (5TB max).", bucketName)
			} else {
				log.Printf("Couldn't upload file %v to %v:%v. Here's why: %v\n",
					fileName, bucketName, objectKey, err)
			}
		} else {
			err = s3.NewObjectExistsWaiter(s3store.client).Wait(
				ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)}, time.Minute)
			if err != nil {
				log.Printf("Failed attempt to wait for object %s to exist.\n", objectKey)
			}
		}
	}
	return err
}

func (s3store *S3Store) UploadLargeObject(ctx context.Context, bucketName string, objectKey string, largeObject []byte) error {
	largeBuffer := bytes.NewReader(largeObject)
	var partMiBs int64 = 10
	uploader := manager.NewUploader(s3store.client, func(u *manager.Uploader) {
		u.PartSize = partMiBs * 1024 * 1024
	})
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   largeBuffer,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"The maximum size for a multipart upload is 5TB.", bucketName)
		} else {
			log.Printf("Couldn't upload large object to %v:%v. Here's why: %v\n",
				bucketName, objectKey, err)
		}
	} else {
		err = s3.NewObjectExistsWaiter(s3store.client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", objectKey)
		}
	}

	return err
}

func (s3store *S3Store) DownloadFile(ctx context.Context, bucketName string, objectKey string, fileName string) error {
	result, err := s3store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", objectKey, bucketName)
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
		}
		return err
	}
	defer result.Body.Close()
	file, err := os.Create(fileName)
	if err != nil {
		log.Printf("Couldn't create file %v. Here's why: %v\n", fileName, err)
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Here's why: %v\n", objectKey, err)
	}
	_, err = file.Write(body)
	return err
}

func (s3store *S3Store) DownloadLargeObject(ctx context.Context, bucketName string, objectKey string) ([]byte, error) {
	var partMiBs int64 = 10
	downloader := manager.NewDownloader(s3store.client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't download large object from %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}
	return buffer.Bytes(), err
}

// CopyToFolder copies an object in a bucket to a subfolder in the same bucket.
func (s3store *S3Store) CopyToFolder(ctx context.Context, bucketName string, objectKey string, folderName string) error {
	objectDest := fmt.Sprintf("%v/%v", folderName, objectKey)
	_, err := s3store.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		CopySource: aws.String(fmt.Sprintf("%v/%v", bucketName, objectKey)),
		Key:        aws.String(objectDest),
	})
	if err != nil {
		var notActive *types.ObjectNotInActiveTierError
		if errors.As(err, &notActive) {
			log.Printf("Couldn't copy object %s from %s because the object isn't in the active tier.\n",
				objectKey, bucketName)
			err = notActive
		}
	} else {
		err = s3.NewObjectExistsWaiter(s3store.client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectDest)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", objectDest)
		}
	}
	return err
}

// CopyToBucket copies an object in a bucket to another bucket.
func (s3store *S3Store) CopyToBucket(ctx context.Context, sourceBucket string, destinationBucket string, objectKey string) error {
	_, err := s3store.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destinationBucket),
		CopySource: aws.String(fmt.Sprintf("%v/%v", sourceBucket, objectKey)),
		Key:        aws.String(objectKey),
	})
	if err != nil {
		var notActive *types.ObjectNotInActiveTierError
		if errors.As(err, &notActive) {
			log.Printf("Couldn't copy object %s from %s because the object isn't in the active tier.\n",
				objectKey, sourceBucket)
			err = notActive
		}
	} else {
		err = s3.NewObjectExistsWaiter(s3store.client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(destinationBucket), Key: aws.String(objectKey)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", objectKey)
		}
	}
	return err
}

// DeleteObjects deletes a list of objects from a bucket.
func (s3store *S3Store) DeleteObjects(ctx context.Context, bucketName string, objectKeys []string) error {
	var objectIds []types.ObjectIdentifier
	for _, key := range objectKeys {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}
	output, err := s3store.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objectIds, Quiet: aws.Bool(true)},
	})
	if err != nil || len(output.Errors) > 0 {
		log.Printf("Error deleting objects from bucket %s.\n", bucketName)
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucketName)
				err = noBucket
			}
		} else if len(output.Errors) > 0 {
			for _, outErr := range output.Errors {
				log.Printf("%s: %s\n", *outErr.Key, *outErr.Message)
			}
			err = fmt.Errorf("%s", *output.Errors[0].Message)
		}
	} else {
		for _, delObjs := range output.Deleted {
			err = s3.NewObjectNotExistsWaiter(s3store.client).Wait(
				ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: delObjs.Key}, time.Minute)
			if err != nil {
				log.Printf("Failed attempt to wait for object %s to be deleted.\n", *delObjs.Key)
			} else {
				log.Printf("Deleted %s.\n", *delObjs.Key)
			}
		}
	}
	return err
}

func (S3Store *S3Store) ListObjectsWithPagination(ctx context.Context, bucket, key string) ([]types.Object, error) {
	paginator := s3.NewListObjectsV2Paginator(S3Store.client, &s3.ListObjectsV2Input{
		Bucket:       aws.String(bucket),
		Prefix:       aws.String(key),
		MaxKeys:      aws.Int32(1000),             // The maximum number of keys that can be returned in a single request is 1000.
		Delimiter:    aws.String("/"),             // Setting a delimiter causes keys that contain the same string between the prefix and the first occurrence of the delimiter to be rolled up into a single result element in CommonPrefixes.
		RequestPayer: types.RequestPayerRequester, // Confirms that the requester knows that they will be charged for the request. Bucket owners need not specify this parameter in their requests.
	})
	var objects []types.Object
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		// 前缀
		for _, prefix := range page.CommonPrefixes {
			fmt.Printf("Prefix: %s\n", *prefix.Prefix)
		}
		objects = append(objects, page.Contents...)
	}
	fmt.Printf("Total objects found: %d\n", len(objects))
	for _, obj := range objects {
		fmt.Printf("%s %10d %s\n", obj.LastModified.Format(time.DateTime), obj.Size, *obj.Key)
	}
	return objects, nil
}

func (s3store *S3Store) GetObject(ctx context.Context, bucketName string, objectKey string) (*s3.GetObjectOutput, error) {
	output, err := s3store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", objectKey, bucketName)
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", bucketName, objectKey, err)
		}
		return nil, err
	}
	return output, nil
}

func (s3store *S3Store) PutObject(ctx context.Context, r io.Reader, bucketName string, objectKey string) (*s3.PutObjectOutput, error) {
	output, err := s3store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   r,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
				"or the multipart upload API (5TB max).", bucketName)
		} else {
			log.Printf("Couldn't upload to %v:%v. Here's why: %v\n",
				bucketName, objectKey, err)
		}
		return nil, err
	}

	err = s3.NewObjectExistsWaiter(s3store.client).Wait(
		ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)}, time.Minute)
	if err != nil {
		log.Printf("Failed attempt to wait for object %s to exist.\n", objectKey)
	}

	return output, err
}

func (s3store *S3Store) UploadObject(ctx context.Context, reader io.Reader, bucketName string, key string) (*manager.UploadOutput, error) {
	return s3store.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})

}

func (s3store *S3Store) HeadObject(ctx context.Context, bucketName string, objectKey string) (*s3.HeadObjectOutput, error) {
	return s3store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
}

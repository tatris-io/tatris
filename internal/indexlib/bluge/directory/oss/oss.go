// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package oss

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const MaxKeySize = 1000

func DefaultClient() (*oss.Client, error) {
	return NewClient(config.Cfg.Directory.OSS.Endpoint,
		config.Cfg.Directory.OSS.AccessKeyID,
		config.Cfg.Directory.OSS.SecretAccessKey)
}

func NewClient(endpoint, accessKeyID, secretAccessKey string) (*oss.Client, error) {
	client, err := oss.New(
		endpoint,
		accessKeyID,
		secretAccessKey,
	)
	if err != nil {
		logger.Error(
			"[oss] new client fail",
			zap.String("endpoint", endpoint),
			zap.Error(err),
		)
		return nil, err
	}
	return client, nil
}

func GetBucket(client *oss.Client, bucketName string) (*oss.Bucket, error) {
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		logger.Error(
			"[oss] get bucket fail",
			zap.String("bucket", bucketName),
			zap.Error(err),
		)
		return nil, err
	}
	return bucket, nil
}

func IsBucketExist(client *oss.Client, bucketName string) (bool, error) {
	result, err := client.GetBucketInfo(bucketName)
	if err != nil {
		logger.Error(
			"[oss] get bucket info fail",
			zap.String("bucket", bucketName),
			zap.Error(err),
		)
		return false, err
	}
	return result.BucketInfo.Name != "", nil
}

func CreateBucket(client *oss.Client, bucketName string) error {
	err := client.CreateBucket(bucketName)
	if err != nil {
		logger.Error(
			"[oss] create bucket fail",
			zap.String("bucket", bucketName),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func ListObjects(
	client *oss.Client,
	bucketName, prefix string,
) ([]oss.ObjectProperties, error) {
	var err error
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return nil, err
	}

	objects := make([]oss.ObjectProperties, 0)

	requested := false
	nextContinuationToken := ""

	for !requested || nextContinuationToken != "" {
		requested = true

		options := make([]oss.Option, 0)
		options = append(options, oss.MaxKeys(MaxKeySize))
		if prefix != "" {
			options = append(options, oss.Prefix(prefix))
		}
		if nextContinuationToken != "" {
			options = append(options, oss.ContinuationToken(nextContinuationToken))
		}
		objectsResult, err := bucket.ListObjectsV2(options...)

		if err != nil {
			logger.Error(
				"[oss] list objects fail",
				zap.String("bucket", bucket.BucketName),
				zap.String("prefix", prefix),
				zap.Error(err),
			)
			return nil, err
		}

		objects = append(objects, objectsResult.Objects...)

		nextContinuationToken = objectsResult.NextContinuationToken
	}

	return objects, nil
}

func GetObject(
	client *oss.Client,
	bucketName, path string,
	minimumConcurrencyLoadSize int,
) ([]byte, error) {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return nil, err
	}

	objMeta, err := bucket.GetObjectMeta(path)
	if err != nil {
		return nil, err
	}
	contentLength := objMeta.Get("Content-Length")
	size, err := strconv.Atoi(contentLength)
	if err != nil {
		return nil, err
	}

	var content []byte
	if size >= minimumConcurrencyLoadSize {
		content, err = GetObjectConcurrency(bucket, size, path)
	} else {
		content, err = GetObjectOrdinary(bucket, path)
	}

	return content, err
}

func GetObjectOrdinary(bucket *oss.Bucket, path string) ([]byte, error) {
	reader, err := bucket.GetObject(path)
	if err != nil {
		logger.Error(
			"[oss] get object fail",
			zap.String("bucket", bucket.BucketName),
			zap.String("path", path),
			zap.Error(err),
		)
		return nil, err
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			logger.Error(
				"oss load close object fail",
				zap.Error(err),
			)
		}
	}()
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func GetObjectConcurrency(bucket *oss.Bucket, size int, path string) ([]byte, error) {
	var content []byte
	content = make([]byte, size)
	gCount := int(math.Min(float64(runtime.GOMAXPROCS(0)), float64(size)))
	partSize := math.Ceil(float64(size) / float64(gCount))

	var eg errgroup.Group
	eg.SetLimit(gCount)

	for i := 0; i < gCount; i++ {
		start := int64(partSize) * int64(i)
		end := int64(math.Min(partSize*(float64(i)+1), float64(size)))
		if start >= int64(size) {
			break
		}
		eg.Go(func() error {
			object, err := bucket.GetObject(path, oss.Range(start, end-1))
			if err != nil {
				logger.Error(
					"[oss] get object fail",
					zap.String("bucket", bucket.BucketName),
					zap.String("path", path),
					zap.String("range", fmt.Sprintf("%d-%d", start, end-1)),
					zap.Error(err),
				)
				return err
			}

			defer object.Close()

			n, err := io.ReadFull(object, content[start:end])
			if err == nil && int64(n) != end-start {
				err = errors.New("read not enough")
			}
			if err != nil {
				logger.Error(
					"[oss] io read part object fail",
					zap.String("bucket", bucket.BucketName),
					zap.String("path", path),
					zap.String("range", fmt.Sprintf("%d-%d", start, end-1)),
					zap.Error(err),
				)
				return err
			}
			return nil
		})
	}
	err := eg.Wait()

	return content, err
}

func PutObject(client *oss.Client, bucketName, path string, buf *bytes.Buffer) error {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return err
	}

	err = bucket.PutObject(path, buf)
	if err != nil {
		logger.Error(
			"[oss] put object fail",
			zap.String("bucket", bucket.BucketName),
			zap.String("path", path),
			zap.Error(err),
		)
		return err
	}

	return nil
}

func DeleteObject(client *oss.Client, bucketName, object string) error {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return err
	}
	err = bucket.DeleteObject(object)
	if err != nil {
		logger.Error(
			"[oss] delete object fail",
			zap.String("bucket", bucket.BucketName),
			zap.String("object", object),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func DeleteObjects(client *oss.Client, bucketName string, objects []string) error {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return err
	}
	_, err = bucket.DeleteObjects(objects, oss.DeleteObjectsQuiet(true))
	if err != nil {
		logger.Error(
			"[oss] delete objects fail",
			zap.String("bucket", bucket.BucketName),
			zap.Any("objects", objects),
			zap.Error(err),
		)
		return err
	}
	return nil
}

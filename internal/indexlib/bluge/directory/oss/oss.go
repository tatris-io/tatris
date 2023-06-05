// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package oss

import (
	"bytes"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"
)

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
	var objectsResult oss.ListObjectsResultV2
	if prefix != "" {
		objectsResult, err = bucket.ListObjectsV2(oss.Prefix(prefix))
	} else {
		objectsResult, err = bucket.ListObjectsV2()
	}
	if err != nil {
		logger.Error(
			"[oss] list objects fail",
			zap.String("bucket", bucket.BucketName),
			zap.String("prefix", prefix),
			zap.Error(err),
		)
		return nil, err
	}
	objects := objectsResult.Objects
	return objects, nil
}

func GetObject(client *oss.Client, bucketName, path string) (io.ReadCloser, error) {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return nil, err
	}
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
	return reader, nil
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

func DeleteObject(client *oss.Client, bucketName, path string) error {
	bucket, err := GetBucket(client, bucketName)
	if err != nil {
		return err
	}
	err = bucket.DeleteObject(path)
	if err != nil {
		logger.Error(
			"[oss] delete object fail",
			zap.String("bucket", bucket.BucketName),
			zap.String("path", path),
			zap.Error(err),
		)
		return err
	}
	return nil
}

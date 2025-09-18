package s3store

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Store struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

func NewS3Client(ctx context.Context, option S3Option) (*S3Store, error) {

	// customResolver2 := s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
	// 	return aws.Endpoint{
	// 		URL:           "http://oss-cn-hangzhou.aliyuncs.com",
	// 		SigningRegion: "cn-hangzhou",
	// 	}, nil
	// })

	// Create custom endpoint resolver
	// customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
	// 	return aws.Endpoint{
	// 		URL: "https://obs.cn-east-3.myhuaweicloud.com",
	// 		// URL:           "http://oss-cn-hangzhou.aliyuncs.com",
	// 		SigningRegion: "cn-hangzhou",
	// 	}, nil
	// })

	// envCredential, err := NewEnvironmentVariableCredentials()
	// if err != nil {
	// 	return nil, err
	// }
	// provider := NewAwsS3Provider(envCredential)

	// Load default config with custom endpoint resolver

	var optFns []func(*config.LoadOptions) error
	if option.Region != "" {
		optFns = append(optFns, config.WithRegion(option.Region))
	}

	conf, err := config.LoadDefaultConfig(ctx, optFns...)
	// config.WithRegion("cn-hangzhou"),
	// config.WithEndpointResolverWithOptions(customResolver),
	// config.WithCredentialsProvider(provider),

	if err != nil {
		return nil, err
	}
	fmt.Printf("option.UsePathStyle: %v\n", option.UsePathStyle)
	client := s3.NewFromConfig(conf,
		func(o *s3.Options) {
			o.UsePathStyle = option.UsePathStyle
			o.Region = option.Region
		},
	)
	uploader := manager.NewUploader(client)
	downloader := manager.NewDownloader(client)

	return &S3Store{
		client:     client,
		uploader:   uploader,
		downloader: downloader,
	}, nil
}

type Credentials struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

func (credentials *Credentials) GetAccessKeyID() string {
	return credentials.AccessKeyId
}

func (credentials *Credentials) GetAccessKeySecret() string {
	return credentials.AccessKeySecret
}

func (credentials *Credentials) GetSecurityToken() string {
	return credentials.SecurityToken
}

func NewAwsS3Provider(credential *Credentials) credentials.StaticCredentialsProvider {
	return credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     credential.AccessKeyId,
			SecretAccessKey: credential.AccessKeySecret,
			SessionToken:    credential.SecurityToken,
		},
	}
}

func NewEnvironmentVariableCredentials() (*Credentials, error) {
	var envCredential *Credentials
	accessID := os.Getenv("OSS_ACCESS_KEY_ID")
	if accessID == "" {
		return envCredential, fmt.Errorf("access key id is empty!")
	}
	accessKey := os.Getenv("OSS_ACCESS_KEY_SECRET")
	if accessKey == "" {
		return envCredential, fmt.Errorf("access key secret is empty!")
	}
	token := os.Getenv("OSS_SESSION_TOKEN")
	envCredential = &Credentials{
		AccessKeyId:     accessID,
		AccessKeySecret: accessKey,
		SecurityToken:   token,
	}

	return envCredential, nil
}

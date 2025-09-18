package s3store

// Options stores configuration for storage.
type S3Option struct {
	// 已实现
	Region       string
	UsePathStyle bool

	// 待实现
	MaxRetries             int
	NoSuchUploadRetryCount int
	Endpoint               string
	NoVerifySSL            bool
	DryRun                 bool
	NoSignRequest          bool
	UseListObjectsV1       bool
	// LogLevel               log.LogLevel
	RequestPayer    string
	Profile         string
	CredentialFile  string
	bucket          string
	AddressingStyle string
}

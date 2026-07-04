# s6cmd

[![Release](https://img.shields.io/github/v/release/LinPr/s6cmd)](https://github.com/LinPr/s6cmd/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/LinPr/s6cmd)](https://golang.org/)
[![License](https://img.shields.io/github/license/LinPr/s6cmd)](LICENSE)

*s6cmd is stiall under developing, please do not use it in your production environment.*

s6cmd is a command-line tool for Amazon S3, using [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2), since the aws has announced the [aws-sdk-go](https://github.com/aws/aws-sdk-go) is deprecated. It's inspired by the popular [s3cmd](https://s3tools.org/s3cmd) tool and aims to provide similar functionality with improved performance and modern Go architecture.

## Features

s6cmd currently supports the following S3 operations:

- **Bucket Operations**
  - `mb` - Make bucket (create S3 bucket)
  - `rb` - Remove bucket (delete S3 bucket)
  - `ls` - List buckets and objects

- **Object Operations**
  - `put` - Upload files to S3
  - `get` - Download files from S3
  - `cp` - Copy objects within S3
  - `mv` - Move/rename objects in S3
  - `rm` - Remove objects from S3
  - `stat` - Display object metadata
  - `du` - Display disk usage for objects
  - `cat` - Print object contents to stdout
  - `head` - Show object head metadata
  - `presign` - Generate a pre-signed URL for an object
  - `pipe` - Stream stdin to a remote object
  - `sync` - Synchronize local/remote directories
  - `tree` - Display bucket/prefix structure as a tree
  - `version` - Print the s6cmd version
  - `bucket-version` - Show bucket versioning configuration



## Installation

### Download Pre-built Binaries

Download the latest release for your platform from the [releases page](https://github.com/LinPr/s6cmd/releases):

```bash
# Linux AMD64
wget https://github.com/LinPr/s6cmd/releases/latest/download/s6cmd-linux-amd64
chmod +x s6cmd-linux-amd64
sudo mv s6cmd-linux-amd64 /usr/local/bin/s6cmd

# macOS ARM64 (Apple Silicon)
wget https://github.com/LinPr/s6cmd/releases/latest/download/s6cmd-darwin-arm64
chmod +x s6cmd-darwin-arm64
sudo mv s6cmd-darwin-arm64 /usr/local/bin/s6cmd
```

### Build from Source

```bash
git clone https://github.com/LinPr/s6cmd.git
cd s6cmd

# Plain build (version prints as "dev")
go build -o s6cmd .

# Stamped build (recommended) — injects the version string printed by `s6cmd version`
VERSION=$(git describe --tags --always)
go build -ldflags "-X github.com/LinPr/s6cmd/version.Version=${VERSION}" -o s6cmd .
```

## Configuration

s6cmd uses AWS credentials and configuration, similar to the AWS CLI. You can configure it using:

### Environment Variables
```bash
export AWS_ENDPOINT_URL_S3=your-object-storage-service-endpoint
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=your-object-storage-region
```

### AWS Credentials File
```bash
aws configure
```

### Configuration File

s6cmd reads a YAML configuration file for default flag values. The file is
searched (in order) at:

- the path passed via `--config /path/to/s6cmd.yaml`
- the path in the `S6CMD_CONFIG` environment variable
- `~/.s6cmd.yaml`
- `./config/s6cmd.yaml`
- `./s6cmd.yaml`

Keys mirror the long flag names. A starter template lives at
[`config/s6cmd.yaml`](config/s6cmd.yaml):

```yaml
endpoint-url: ""
region: ""
profile: ""
no-verify-ssl: false
no-paginate: false
output: text
path-style: false
addressing-style: ""  # path | virtual | auto
```

### Configuration Precedence

For every shared flag s6cmd resolves settings in this order, highest priority
first:

1. **command-line flag** (e.g. `--region us-east-1`)
2. **environment variable** (`AWS_REGION`, `AWS_PROFILE`, `AWS_ENDPOINT_URL_S3`,
   `AWS_NO_VERIFY_SSL`, `AWS_NO_PAGINATE`, `AWS_OUTPUT`, `S6CMD_USE_PATH_STYLE`,
   `S3_ADDRESSING_STYLE`, `S6CMD_CONFIG`)
3. **config file** value
4. cobra flag default

This mirrors the AWS CLI's behaviour: explicit flags win, env comes next,
config file after that, and the built-in default is the last resort.

### Addressing Style

S3 supports two URL styles for addressing buckets. s6cmd lets you pick via
`--addressing-style` (env `S3_ADDRESSING_STYLE`):

| Style    | URL shape                            | When to use                              |
|----------|--------------------------------------|------------------------------------------|
| `path`   | `https://endpoint/bucket/key`        | MinIO, Alibaba OSS, Tencent COS, GCS XML |
| `virtual`| `https://bucket.endpoint/key`        | AWS S3 (default), virtual-host services  |
| `auto`   | endpoint-derived (default)           | Best for mixed setups                    |

`auto` (the default when the flag is empty) picks `virtual` when no
`--endpoint-url` is set (i.e. you are talking to AWS S3 directly) and `path`
when a custom endpoint is set, which is what MinIO/OSS/COS/GCS expect. An
explicit `--addressing-style=path|virtual` always wins over the auto rule.

The legacy `--path-style` flag (env `S6CMD_USE_PATH_STYLE`) is kept for
backwards compatibility and is equivalent to `--addressing-style=path`. When
both are set, `--addressing-style` takes precedence.

S3 Transfer Acceleration is auto-detected: if `--endpoint-url` points at
`s3-accelerate.amazonaws.com`, s6cmd enables `UseAccelerate` and lets the
SDK own the endpoint. A `storage.googleapis.com` endpoint is detected as
GCS so callers can branch on it (GCS defaults to path-style).

##  Usage

### Basic Examples

```bash
# List all buckets
s6cmd ls

# List objects in a bucket
s6cmd ls s3://my-bucket/

# Upload a file
s6cmd put local-file.txt s3://my-bucket/remote-file.txt

# Download a file
s6cmd get s3://my-bucket/remote-file.txt local-file.txt

# Copy objects within S3
s6cmd cp s3://source-bucket/file.txt s3://dest-bucket/file.txt

# Sync local directory with S3
s6cmd sync ./local-dir/ s3://my-bucket/prefix/

# Display bucket structure
s6cmd tree s3://my-bucket/

# Create a new bucket
s6cmd mb s3://my-new-bucket

# Remove objects
s6cmd rm s3://my-bucket/file.txt

# Show object statistics
s6cmd stat s3://my-bucket/file.txt
```

### Global Flags

```bash
# Dry run mode (show what would be done without executing)
s6cmd put -n local-file.txt s3://my-bucket/file.txt

# Get help for any command
s6cmd help
s6cmd put --help
```

## Architecture

s6cmd is built using:

- **[AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)** - Modern, efficient AWS SDK
- **[Cobra](https://github.com/spf13/cobra)** - CLI framework for Go
- **[Viper](https://github.com/spf13/viper)** - Configuration management


## Development Status

s6cmd is actively under development. While it already provides essential S3 operations, we're continuously adding new features and improvements.



##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments

- Inspired by [s3cmd](https://s3tools.org/s3cmd) - The original S3 command-line tool
- Built with the excellent [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)



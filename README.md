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
go build -o s6cmd .
```

## Configuration

s6cmd uses AWS credentials and configuration, similar to the AWS CLI. You can configure it using:

### Environment Variables
```bash
export export AWS_ENDPOINT_URL_S3=your-object-storage-service-endpoint
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=your-object-storage-region
```

### AWS Credentials File
```bash
aws configure
```

### Configuration File
Create a configuration file at `config/s6cmd.yaml`:



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



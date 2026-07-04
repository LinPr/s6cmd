# s6cmd

[![Version](https://img.shields.io/github/v/release/LinPr/s6cmd?include_prereleases)](https://github.com/LinPr/s6cmd/releases/tag/v0.0.4)
[![Go Version](https://img.shields.io/github/go-mod/go-version/LinPr/s6cmd)](https://golang.org/)
[![License](https://img.shields.io/github/license/LinPr/s6cmd)](LICENSE)

*This is a pre-release version (v0.0.4). s6cmd is still under active development — please do not use it in a production environment.*

s6cmd is a command-line tool for Amazon S3, using [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2), since AWS has announced that [aws-sdk-go](https://github.com/aws/aws-sdk-go) is deprecated. It's inspired by popular S3 command-line tools and aims to provide similar functionality with improved performance and a modern Go architecture.

## Current Version

**v0.0.4** — see [releases](https://github.com/LinPr/s6cmd/releases) for the changelog.
The version printed by `s6cmd version` is stamped at build time from the git
tag (`git describe --tags --always`); an unstamped `go build .` reports `dev`.

## Features

s6cmd currently supports the following S3 operations (20 commands):

- **Bucket Operations**
  - `mb` - Make bucket (create S3 bucket)
  - `rb` - Remove bucket (delete S3 bucket, `--force` empties it first)
  - `ls` - List buckets and objects (`--recursive`, `--humanize`, `--summarize`,
    `--etag`, `--storage-class`, `--show-fullpath`, `--all-versions`)
  - `bucket-version` - Show or set bucket versioning (`--set Enabled|Suspended`)

- **Object Operations**
  - `put` - Upload files to S3 (supports `-` for stdin)
  - `get` - Download files from S3 (`--recursive`, `--jobs`)
  - `cp` - Copy objects (S3↔S3 / S3↔local / local↔local), with
    `--no-clobber`, `--if-size-differ`, `--if-source-newer`, `--flatten`,
    `--exclude`/`--include`, `--storage-class`, `--metadata`, `--sse`,
    `--concurrency`, `--part-size`, and more shared flags
  - `mv` - Move/rename objects (Copy + Delete source, same shared flags as `cp`)
  - `rm` - Remove objects (`--recursive`, `--exclude`/`--include`,
    `--all-versions`, `--version-id`)
  - `sync` - Synchronize local/remote directories (`--delete`, `--size-only`,
    `--exit-on-error`)
  - `stat` - Display object metadata (human-readable)
  - `du` - Display disk usage for objects (`--group`, `--humanize`, `--exclude`)
  - `cat` - Print object contents to stdout (streamed, supports wildcards)
  - `head` - Show object/bucket head metadata as JSON
  - `presign` - Generate a pre-signed URL for an object (`--expire`)
  - `pipe` - Stream stdin to a remote object
  - `tree` - Display bucket/prefix structure as a tree
  - `select` - Run SQL queries against S3 objects (`csv` / `json` / `parquet`
    subcommands)
  - `run` - Execute a batch of commands from a file or stdin
  - `version` - Print the s6cmd version



## Installation

### Download Pre-built Binaries

Download the latest release (currently **v0.0.4**) for your platform from the
[releases page](https://github.com/LinPr/s6cmd/releases):

```bash
# Linux AMD64
wget https://github.com/LinPr/s6cmd/releases/download/v0.0.4/s6cmd-linux-amd64
chmod +x s6cmd-linux-amd64
sudo mv s6cmd-linux-amd64 /usr/local/bin/s6cmd

# macOS ARM64 (Apple Silicon)
wget https://github.com/LinPr/s6cmd/releases/download/v0.0.4/s6cmd-darwin-arm64
chmod +x s6cmd-darwin-arm64
sudo mv s6cmd-darwin-arm64 /usr/local/bin/s6cmd
```

### Build from Source

```bash
git clone https://github.com/LinPr/s6cmd.git
cd s6cmd
git checkout v0.0.4   # pin to a tagged release; drop this line for the tip of main

# Plain build (version prints as "dev")
go build -o s6cmd .

# Stamped build (recommended) — injects the version string printed by `s6cmd version`
VERSION=$(git describe --tags --always)   # → v0.0.4 (or v0.0.4-N-gXXXX if ahead)
go build -ldflags "-X github.com/LinPr/s6cmd/version.Version=${VERSION}" -o s6cmd .
```

Or, with [Task](https://taskfile.dev):

```bash
task build      # version + commit stamped automatically
task release    # cross-compile all platforms into dist/
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
   `S3_ADDRESSING_STYLE`, `AWS_RETRY_COUNT`, `AWS_SHARED_CREDENTIALS_FILE`,
   `S6CMD_CONFIG`)
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

# List objects in a bucket (recursive, human-readable sizes)
s6cmd ls --recursive --humanize s3://my-bucket/

# Upload a file
s6cmd put local-file.txt s3://my-bucket/remote-file.txt

# Upload from stdin
cat local-file.txt | s6cmd put - s3://my-bucket/remote-file.txt

# Download a file
s6cmd get s3://my-bucket/remote-file.txt local-file.txt

# Copy objects (server-side, within S3)
s6cmd cp s3://source-bucket/file.txt s3://dest-bucket/file.txt

# Copy with shared transfer flags
s6cmd cp --concurrency 8 --part-size 64 s3://src/file s3://dst/file

# Move objects (copy + delete source)
s6cmd mv --recursive s3://source-bucket/prefix/ s3://dest-bucket/prefix/

# Sync local directory with S3 (delete extra files at destination)
s6cmd sync --delete ./local-dir/ s3://my-bucket/prefix/

# Wildcard copy
s6cmd cp "s3://my-bucket/logs/*.log" ./logs/

# Display bucket structure
s6cmd tree s3://my-bucket/

# Create a new bucket
s6cmd mb s3://my-new-bucket

# Remove objects
s6cmd rm s3://my-bucket/file.txt
s6cmd rm --recursive s3://my-bucket/prefix/

# Show object statistics / head
s6cmd stat s3://my-bucket/file.txt
s6cmd head s3://my-bucket/file.txt

# Print object contents
s6cmd cat s3://my-bucket/file.txt

# Disk usage
s6cmd du --humanize s3://my-bucket/

# Pre-signed URL
s6cmd presign --expire 1h s3://my-bucket/file.txt

# Stream stdin to a remote object
echo '{"k":1}' | s6cmd pipe s3://my-bucket/data.json

# SQL query against S3 objects
s6cmd select json --query "SELECT * FROM s3object s" s3://my-bucket/data.json

# Run a batch of commands from a file
s6cmd run commands.txt

# Print version
s6cmd version
```

### Global Flags

Common flags that apply to every subcommand:

| Flag | Env | Description |
|---|---|---|
| `--endpoint-url` | `AWS_ENDPOINT_URL_S3` | Custom S3 endpoint (MinIO/OSS/COS/GCS) |
| `--region` | `AWS_REGION` | AWS region; auto-detected if empty |
| `--profile` | `AWS_PROFILE` | Named profile from credentials file |
| `--credentials-file` | `AWS_SHARED_CREDENTIALS_FILE` | Override credentials file path |
| `--no-sign-request` | — | Anonymous (unsigned) requests; mutually exclusive with `--profile`/`--credentials-file` |
| `--addressing-style` | `S3_ADDRESSING_STYLE` | `path` / `virtual` / `auto` (see [Addressing Style](#addressing-style)) |
| `--path-style` | `S6CMD_USE_PATH_STYLE` | Legacy alias for `--addressing-style=path` |
| `--no-verify-ssl` | `AWS_NO_VERIFY_SSL` | Skip TLS verification |
| `--no-paginate` | `AWS_NO_PAGINATE` | Disable automatic pagination |
| `--output` | `AWS_OUTPUT` | `text` / `json` / `table` |
| `--retry-count` | `AWS_RETRY_COUNT` | Max retries per request (default 10) |
| `--config` | `S6CMD_CONFIG` | Path to a YAML config file |

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

s6cmd is actively under development (current release: **v0.0.4**). It already
gofakes3, and unit tests for the storage layer over an httptest mock server.
New features and improvements are still being added — please report issues at
the [GitHub issue tracker](https://github.com/LinPr/s6cmd/issues).



##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

##  Acknowledgments

- Inspired by the broader ecosystem of S3 command-line tools
- Built with the excellent [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)



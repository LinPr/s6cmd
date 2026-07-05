# s6cmd

[![Go Version](https://img.shields.io/github/go-mod/go-version/LinPr/s6cmd)](https://golang.org/)
[![License](https://img.shields.io/github/license/LinPr/s6cmd)](LICENSE)


s6cmd is a command-line tool for Amazon S3, built on [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2). It is inspired by popular S3 CLI tools and aims for similar functionality with improved performance and a modern Go architecture.

## Features

20 commands covering bucket and object operations:

### Bucket Operations
- `mb` — create bucket
- `rb` — remove bucket
- `ls` — list buckets/objects (`--recursive`, `--humanize`, `--summarize`, `--etag`, `--storage-class`, `--show-fullpath`, `--all-versions`)
- `bucket-version` — manage bucket versioning (`--set Enabled|Suspended`)

### Object Operations
- `put` — upload object (stdin with `-`)
- `get` — download object (`--recursive`, `--jobs`)
- `cp` — copy S3↔S3 / S3↔local (`--no-clobber`, `--if-size-differ`, `--if-source-newer`, `--flatten`, `--exclude`/`--include`, `--storage-class`, `--metadata`, `--sse`, `--concurrency`, `--part-size`)
- `mv` — move object (copy + delete, same shared flags as `cp`)
- `rm` — delete object (`--recursive`, `--exclude`/`--include`, `--all-versions`, `--version-id`)
- `sync` — sync directories (`--delete`, `--size-only`, `--exit-on-error`)
- `stat` — object metadata
- `du` — disk usage (`--group`, `--humanize`, `--exclude`)
- `cat` — stream object content (supports wildcards)
- `head` — show object metadata (JSON)
- `presign` — generate presigned URL (`--expire`)
- `pipe` — upload from stdin
- `tree` — tree view of bucket
- `select` — SQL query on object (`csv`/`json`/`parquet`)
- `run` — batch commands from file/stdin
- `version` — show version

## Installation

Download the latest release from the [releases page](https://github.com/LinPr/s6cmd/releases):

```bash
# Linux AMD64
wget https://github.com/LinPr/s6cmd/releases/download/v0.0.4/s6cmd-linux-amd64
chmod +x s6cmd-linux-amd64 && sudo mv s6cmd-linux-amd64 /usr/local/bin/s6cmd
```

Or build from source:

```bash
git clone https://github.com/LinPr/s6cmd.git && cd s6cmd
go build -o s6cmd .
# Stamped build (recommended): injects the version printed by `s6cmd version`
go build -ldflags "-X github.com/LinPr/s6cmd/version.Version=$(git describe --tags --always)" -o s6cmd .
```

With [Task](https://taskfile.dev): `task build` (stamped) or `task release` (cross-compile).

## Configuration

s6cmd uses AWS credentials and configuration, similar to the AWS CLI:

```bash
export AWS_ENDPOINT_URL_S3=your-endpoint
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=your-region
```

Or `aws configure`, or a YAML config file. The file is searched (in order) at: the path from `--config`, the `S6CMD_CONFIG` env var, `~/.s6cmd.yaml`, `./config/s6cmd.yaml`, `./s6cmd.yaml`. Keys mirror the long flag names; a starter template lives at [`config/s6cmd.yaml`](config/s6cmd.yaml).

### Configuration Precedence

For every shared flag, highest priority first:

1. **command-line flag** (e.g. `--region us-east-1`)
2. **environment variable** (`AWS_REGION`, `AWS_PROFILE`, `AWS_ENDPOINT_URL_S3`, `AWS_NO_VERIFY_SSL`, `AWS_NO_PAGINATE`, `AWS_OUTPUT`, `S6CMD_USE_PATH_STYLE`, `AWS_RETRY_COUNT`, `AWS_SHARED_CREDENTIALS_FILE`, `S6CMD_CONFIG`)
3. **config file** value
4. cobra flag default

This mirrors the AWS CLI: explicit flags win, env comes next, config file after that, built-in default last.

### Addressing Style

Use `--path-style` (env `S6CMD_USE_PATH_STYLE`) to force path-style (`https://endpoint/bucket/key`), required for MinIO, Alibaba OSS, Tencent COS, and GCS. Without it, virtual-host style (`https://bucket.endpoint/key`) is used, which is the AWS S3 default.

S3 Transfer Acceleration is auto-detected: if `--endpoint-url` points at `s3-accelerate.amazonaws.com`, s6cmd enables `UseAccelerate` and lets the SDK own the endpoint.

## Usage

```bash
s6cmd ls                                       # list all buckets
s6cmd ls --recursive --humanize s3://my-bucket/
s6cmd put local-file.txt s3://my-bucket/remote-file.txt
cat local-file.txt | s6cmd put - s3://my-bucket/remote-file.txt   # stdin
s6cmd get s3://my-bucket/remote-file.txt local-file.txt
s6cmd cp s3://src-bucket/file.txt s3://dst-bucket/file.txt        # server-side
s6cmd cp --concurrency 8 --part-size 64 s3://src/file s3://dst/file
s6cmd mv --recursive s3://src-bucket/prefix/ s3://dst-bucket/prefix/
s6cmd sync --delete ./local-dir/ s3://my-bucket/prefix/
s6cmd cp "s3://my-bucket/logs/*.log" ./logs/                       # wildcard
s6cmd tree s3://my-bucket/
s6cmd mb s3://my-new-bucket
s6cmd rm --recursive s3://my-bucket/prefix/
s6cmd stat s3://my-bucket/file.txt
s6cmd cat s3://my-bucket/file.txt
s6cmd du --humanize s3://my-bucket/
s6cmd presign --expire 1h s3://my-bucket/file.txt
echo '{"k":1}' | s6cmd pipe s3://my-bucket/data.json
s6cmd select json --query "SELECT * FROM s3object s" s3://my-bucket/data.json
s6cmd run commands.txt
s6cmd version
```

### Global Flags

| Flag | Env | Description |
|---|---|---|
| `--endpoint-url` | `AWS_ENDPOINT_URL_S3` | Custom S3 endpoint (MinIO/OSS/COS/GCS) |
| `--region` | `AWS_REGION` | AWS region; auto-detected if empty |
| `--profile` | `AWS_PROFILE` | Named profile from credentials file |
| `--credentials-file` | `AWS_SHARED_CREDENTIALS_FILE` | Override credentials file path |
| `--no-sign-request` | — | Anonymous (unsigned) requests; mutually exclusive with `--profile`/`--credentials-file` |
| `--path-style` | `S6CMD_USE_PATH_STYLE` | Force path-style addressing (MinIO/OSS/COS/GCS) |
| `--no-verify-ssl` | `AWS_NO_VERIFY_SSL` | Skip TLS verification |
| `--no-paginate` | `AWS_NO_PAGINATE` | Disable automatic pagination |
| `--output` | `AWS_OUTPUT` | `text` / `json` / `table` |
| `--retry-count` | `AWS_RETRY_COUNT` | Max retries per request (default 10) |
| `--config` | `S6CMD_CONFIG` | Path to a YAML config file |

```bash
s6cmd put -n local-file.txt s3://my-bucket/file.txt   # dry run
s6cmd put --help                                      # help for any command
```

## Architecture

Built with [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2), [Cobra](https://github.com/spf13/cobra), and [Viper](https://github.com/spf13/viper).

## Development Status

Pre-release (v0.0.4). The e2e suite runs over [gofakes3](https://github.com/igungor/gofakes3) and the storage layer has unit tests over an httptest mock server. Please report issues at the [GitHub issue tracker](https://github.com/LinPr/s6cmd/issues).

## License

MIT License — see [LICENSE](LICENSE).

## Acknowledgments

- Inspired by the broader ecosystem of S3 command-line tools
- Built with the [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)

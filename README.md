# s32tar

A Python library for streaming files from AWS S3 folders into TAR archives using the tarfile module.

## Features

- Stream S3 objects directly into TAR archives without downloading files locally first
- Support for multiple compression formats (gzip, bz2, xz)
- Configurable prefix stripping for cleaner archive paths
- Pagination support for large S3 buckets
- **Automatic chunking** to split large archives into multiple files (configurable size limit)

## Installation

```bash
pip install s32tar
```

Or install from source:

```bash
pip install -e .
```

## Usage

### Basic Usage

```python
from s32tar import S3TarArchiver

# Create an archiver for a specific S3 bucket
archiver = S3TarArchiver(bucket="my-bucket")

# Archive all files from an S3 prefix to a local file
file_count = archiver.archive_to_file(
    prefix="data/exports/",
    output_path="archive.tar"
)
print(f"Archived {file_count} files")
```

### Streaming to a File Object

```python
import io
from s32tar import S3TarArchiver

archiver = S3TarArchiver(bucket="my-bucket")

# Stream to any file-like object
output = io.BytesIO()
file_count = archiver.stream_to_tar(
    prefix="data/exports/",
    output=output
)
```

### With Compression

```python
from s32tar import S3TarArchiver

archiver = S3TarArchiver(bucket="my-bucket")

# Create a compressed archive (supports 'gz', 'bz2', 'xz')
archiver.archive_to_file(
    prefix="data/exports/",
    output_path="archive.tar.gz",
    compression="gz"
)
```

### Keep Original S3 Paths

```python
from s32tar import S3TarArchiver

archiver = S3TarArchiver(bucket="my-bucket")

# Keep the full S3 key paths in the archive
archiver.archive_to_file(
    prefix="data/exports/",
    output_path="archive.tar",
    strip_prefix=False  # Files will be at data/exports/... in the archive
)
```

### Custom S3 Client

```python
import boto3
from s32tar import S3TarArchiver

# Use a custom S3 client (e.g., for different credentials or region)
s3_client = boto3.client(
    "s3",
    region_name="eu-west-1",
    aws_access_key_id="YOUR_ACCESS_KEY",
    aws_secret_access_key="YOUR_SECRET_KEY"
)

archiver = S3TarArchiver(bucket="my-bucket", s3_client=s3_client)
```

### Chunking Large Archives

Automatically split large archives into multiple files with a configurable size limit (default: 20GB):

```python
from s32tar import S3TarArchiver

# Set maximum archive size to 10GB
archiver = S3TarArchiver(bucket="my-bucket", max_size_gb=10.0)

# Create chunked archives (archive_1.tar, archive_2.tar, ...)
chunks = archiver.archive_to_files_chunked(
    prefix="data/exports/",
    output_pattern="archive_{}.tar"
)

# Returns: [("archive_1.tar", 150), ("archive_2.tar", 142), ...]
# Each tuple is (filename, file_count)
for filename, file_count in chunks:
    print(f"{filename}: {file_count} files")
```

Chunking also works with compression:

```python
# Create compressed chunks (archive_1.tar.gz, archive_2.tar.gz, ...)
chunks = archiver.archive_to_files_chunked(
    prefix="data/exports/",
    output_pattern="archive_{}.tar.gz",
    compression="gz"
)
```

## Development

### Setup

```bash
# Install development dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest tests/ -v
```

### Run Linter

```bash
ruff check src/ tests/
ruff format src/ tests/
```

## License

MIT
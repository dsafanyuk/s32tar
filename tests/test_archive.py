"""Unit tests for the S3TarArchiver module."""

import io
import tarfile
import tempfile

import boto3
import pytest
from moto import mock_aws

from s32tar.archive import S3TarArchiver


@pytest.fixture
def s3_bucket():
    """Create a mock S3 bucket with test files."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)

        # Create test files in the bucket
        test_files = {
            "folder1/file1.txt": b"Hello, World!",
            "folder1/file2.txt": b"Test content for file 2",
            "folder1/subfolder/file3.txt": b"Nested file content",
            "folder2/other.txt": b"Other folder content",
        }

        for key, content in test_files.items():
            s3.put_object(Bucket=bucket_name, Key=key, Body=content)

        yield s3, bucket_name, test_files


class TestS3TarArchiver:
    """Test cases for S3TarArchiver."""

    def test_list_objects(self, s3_bucket):
        """Test listing objects from S3 with a prefix."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        objects = archiver.list_objects("folder1/")

        assert len(objects) == 3
        keys = [obj["Key"] for obj in objects]
        assert "folder1/file1.txt" in keys
        assert "folder1/file2.txt" in keys
        assert "folder1/subfolder/file3.txt" in keys

    def test_list_objects_empty_prefix(self, s3_bucket):
        """Test listing all objects in the bucket."""
        s3, bucket_name, test_files = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        objects = archiver.list_objects("")

        assert len(objects) == len(test_files)

    def test_stream_to_tar(self, s3_bucket):
        """Test streaming S3 files to a TAR archive."""
        s3, bucket_name, test_files = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        file_count = archiver.stream_to_tar(prefix="folder1/", output=output)

        assert file_count == 3

        # Verify the TAR archive contents
        output.seek(0)
        with tarfile.open(fileobj=output, mode="r|") as tar:
            members = []
            for member in tar:
                members.append(member)
            names = [m.name for m in members]

        assert len(names) == 3
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "subfolder/file3.txt" in names

    def test_stream_to_tar_no_strip_prefix(self, s3_bucket):
        """Test streaming without stripping the prefix."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        file_count = archiver.stream_to_tar(
            prefix="folder1/", output=output, strip_prefix=False
        )

        assert file_count == 3

        output.seek(0)
        with tarfile.open(fileobj=output, mode="r|") as tar:
            names = [member.name for member in tar]

        assert "folder1/file1.txt" in names
        assert "folder1/file2.txt" in names
        assert "folder1/subfolder/file3.txt" in names

    def test_stream_to_tar_with_gzip_compression(self, s3_bucket):
        """Test streaming with gzip compression."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        file_count = archiver.stream_to_tar(
            prefix="folder1/", output=output, compression="gz"
        )

        assert file_count == 3

        output.seek(0)
        with tarfile.open(fileobj=output, mode="r:gz") as tar:
            names = [member.name for member in tar]

        assert len(names) == 3

    def test_stream_to_tar_invalid_compression(self, s3_bucket):
        """Test that invalid compression raises an error."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        with pytest.raises(ValueError, match="Invalid compression"):
            archiver.stream_to_tar(
                prefix="folder1/", output=output, compression="invalid"
            )

    def test_stream_to_tar_empty_folder(self, s3_bucket):
        """Test streaming from a non-existent prefix."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        file_count = archiver.stream_to_tar(prefix="nonexistent/", output=output)

        assert file_count == 0

    def test_archive_to_file(self, s3_bucket):
        """Test archiving to a file path."""
        s3, bucket_name, _ = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)

        with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as f:
            output_path = f.name

        file_count = archiver.archive_to_file(
            prefix="folder1/", output_path=output_path
        )

        assert file_count == 3

        # Verify the TAR file contents
        with tarfile.open(output_path, mode="r") as tar:
            names = tar.getnames()

        assert len(names) == 3

    def test_file_content_integrity(self, s3_bucket):
        """Test that file contents are preserved in the archive."""
        s3, bucket_name, test_files = s3_bucket

        archiver = S3TarArchiver(bucket=bucket_name, s3_client=s3)
        output = io.BytesIO()

        archiver.stream_to_tar(prefix="folder1/", output=output)

        output.seek(0)
        with tarfile.open(fileobj=output, mode="r") as tar:
            file1 = tar.extractfile("file1.txt")
            assert file1 is not None
            content = file1.read()
            assert content == test_files["folder1/file1.txt"]

    def test_default_s3_client(self):
        """Test that a default S3 client is created if not provided."""
        with mock_aws():
            archiver = S3TarArchiver(bucket="test-bucket")
            assert archiver.s3_client is not None
            assert archiver.bucket == "test-bucket"

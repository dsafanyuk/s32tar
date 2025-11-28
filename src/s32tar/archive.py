"""Core module for streaming S3 files into a TAR archive."""

import tarfile
from typing import BinaryIO

import boto3


class S3TarArchiver:
    """Stream files from an S3 folder into a TAR archive."""

    def __init__(self, bucket: str, s3_client=None):
        """Initialize the S3TarArchiver.

        Args:
            bucket: The name of the S3 bucket.
            s3_client: Optional boto3 S3 client. If not provided,
                a new client will be created.
        """
        self.bucket = bucket
        self.s3_client = s3_client or boto3.client("s3")

    def list_objects(self, prefix: str) -> list[dict]:
        """List all objects in the S3 bucket with the given prefix.

        Args:
            prefix: The S3 key prefix (folder path) to list objects from.

        Returns:
            A list of dictionaries containing 'Key' and 'Size' for each object.
        """
        objects = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Skip "folder" objects (keys ending with /)
                    if not obj["Key"].endswith("/"):
                        objects.append({"Key": obj["Key"], "Size": obj["Size"]})

        return objects

    def stream_to_tar(
        self,
        prefix: str,
        output: BinaryIO,
        strip_prefix: bool = True,
        compression: str | None = None,
    ) -> int:
        """Stream S3 objects from a prefix into a TAR archive.

        Args:
            prefix: The S3 key prefix (folder path) to archive.
            output: A file-like object to write the TAR archive to.
            strip_prefix: If True, remove the prefix from file names in the archive.
            compression: Optional compression mode ('gz', 'bz2', 'xz', or None).

        Returns:
            The number of files added to the archive.
        """
        # Determine tar mode based on compression
        mode = "w|"
        if compression:
            if compression not in ("gz", "bz2", "xz"):
                raise ValueError(
                    f"Invalid compression: {compression}. "
                    "Must be 'gz', 'bz2', 'xz', or None."
                )
            mode += compression

        objects = self.list_objects(prefix)
        file_count = 0

        with tarfile.open(fileobj=output, mode=mode) as tar:
            for obj in objects:
                key = obj["Key"]
                size = obj["Size"]

                # Determine the name in the archive
                if strip_prefix and key.startswith(prefix):
                    archive_name = key[len(prefix) :].lstrip("/")
                else:
                    archive_name = key

                # Skip if the resulting name is empty
                if not archive_name:
                    continue

                # Stream the S3 object
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                body = response["Body"]

                try:
                    # Create a TarInfo object
                    tar_info = tarfile.TarInfo(name=archive_name)
                    tar_info.size = size

                    # Add the file to the archive
                    tar.addfile(tar_info, fileobj=body)
                    file_count += 1
                finally:
                    body.close()

        return file_count

    def archive_to_file(
        self,
        prefix: str,
        output_path: str,
        strip_prefix: bool = True,
        compression: str | None = None,
    ) -> int:
        """Archive S3 objects from a prefix to a TAR file.

        Args:
            prefix: The S3 key prefix (folder path) to archive.
            output_path: The path to write the TAR archive to.
            strip_prefix: If True, remove the prefix from file names in the archive.
            compression: Optional compression mode ('gz', 'bz2', 'xz', or None).

        Returns:
            The number of files added to the archive.
        """
        with open(output_path, "wb") as f:
            return self.stream_to_tar(
                prefix=prefix,
                output=f,
                strip_prefix=strip_prefix,
                compression=compression,
            )

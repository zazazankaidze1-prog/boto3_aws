import io
import logging
import mimetypes
import os
from hashlib import md5
from time import localtime
from urllib.request import urlopen

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError


SMALL_FILE_LIMIT = 100 * 1024 * 1024
ALLOWED_MIMETYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "application/pdf",
    "text/plain",
    "text/csv",
    "application/json",
    "application/zip",
    "application/octet-stream",
    "video/mp4",
    "audio/mpeg",
}


def get_objects(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.list_objects_v2(Bucket=bucket_name)
        contents = response.get("Contents", [])
        if not contents:
            print(f"Bucket '{bucket_name}' is empty.")
            return []
        for key in contents:
            print(f"  {key['Key']}, size: {key['Size']} bytes")
        return contents
    except ClientError as e:
        logging.error(e)
        return []


def validate_mimetype(file_path, allowed=None):
    """Guess mimetype and check against the allowlist. Rejects unknown types."""
    allowed = allowed or ALLOWED_MIMETYPES
    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type is None:
        print(f"Rejected: unknown mimetype for '{file_path}'")
        return None
    if mime_type not in allowed:
        print(f"Rejected: mimetype '{mime_type}' is not allowed.")
        return None
    return mime_type


def upload_small_file(aws_s3_client, bucket_name, file_path, key=None, validate_mime=True):
    """
    Upload a file < 100 MB with a single PUT request (upload_file).
    Uses mimetype validation by default.
    """
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return False

    size = os.path.getsize(file_path)
    if size >= SMALL_FILE_LIMIT:
        print(
            f"File is {size} bytes ({size / 1024 / 1024:.2f} MB). "
            f"Use upload_large_file for files >= 100 MB."
        )
        return False

    mime_type = "application/octet-stream"
    if validate_mime:
        mime_type = validate_mimetype(file_path)
        if mime_type is None:
            return False

    key = key or os.path.basename(file_path)
    try:
        aws_s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=key,
            ExtraArgs={"ContentType": mime_type},
        )
        print(f"Uploaded (small): {file_path} -> s3://{bucket_name}/{key}")
        return True
    except ClientError as e:
        logging.error(e)
        return False


def upload_large_file(aws_s3_client, bucket_name, file_path, key=None, validate_mime=True):
    """
    Upload a file of any size using multipart transfer.
    Threshold / chunk size: 25 MB. Uses up to 10 parallel threads.
    """
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return False

    mime_type = "application/octet-stream"
    if validate_mime:
        mime_type = validate_mimetype(file_path)
        if mime_type is None:
            return False

    key = key or os.path.basename(file_path)
    config = TransferConfig(
        multipart_threshold=25 * 1024 * 1024,
        multipart_chunksize=25 * 1024 * 1024,
        max_concurrency=10,
        use_threads=True,
    )

    try:
        aws_s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=key,
            ExtraArgs={"ContentType": mime_type},
            Config=config,
        )
        size_mb = os.path.getsize(file_path) / 1024 / 1024
        print(f"Uploaded (multipart, {size_mb:.2f} MB): {file_path} -> s3://{bucket_name}/{key}")
        return True
    except ClientError as e:
        logging.error(e)
        return False


def download_file_and_upload_to_s3(aws_s3_client, bucket_name, url, keep_local=False) -> str:
    file_name = f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.jpg'
    with urlopen(url) as response:
        content = response.read()
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            ExtraArgs={"ContentType": "image/jpg"},
            Key=file_name,
        )
    if keep_local:
        with open(file_name, mode="wb") as jpg_file:
            jpg_file.write(content)

    region = aws_s3_client.meta.region_name or "us-east-1"
    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_name}"


def upload_file(aws_s3_client, filename, bucket_name):
    """Kept for backwards compatibility with the original script."""
    try:
        aws_s3_client.upload_file(filename, bucket_name, os.path.basename(filename))
        return True
    except ClientError as e:
        logging.error(e)
        return False


def upload_file_obj(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.upload_fileobj(file, bucket_name, os.path.basename(filename))


def upload_file_put(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.put_object(
            Bucket=bucket_name, Key=os.path.basename(filename), Body=file.read()
        )


def delete_object(aws_s3_client, bucket_name, key):
    try:
        aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
        print(f"Deleted: s3://{bucket_name}/{key}")
        return True
    except ClientError as e:
        logging.error(e)
        return False


def list_file_versions(aws_s3_client, bucket_name, key):
    """Return all versions of a given object key, newest first."""
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
        versions = [v for v in response.get("Versions", []) if v["Key"] == key]

        if not versions:
            print(f"No versions found for '{key}' in bucket '{bucket_name}'.")
            return []

        print(f"File '{key}' has {len(versions)} version(s):")
        for i, v in enumerate(versions, start=1):
            marker = " (latest)" if v.get("IsLatest") else ""
            print(
                f"  #{i}{marker}  VersionId={v['VersionId']}  "
                f"LastModified={v['LastModified'].isoformat()}  "
                f"Size={v['Size']}"
            )
        return versions
    except ClientError as e:
        logging.error(e)
        return []


def restore_previous_version(aws_s3_client, bucket_name, key):
    """Copy the second-newest version over the current one as a new version."""
    try:
        response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
        versions = [v for v in response.get("Versions", []) if v["Key"] == key]

        if len(versions) < 2:
            print(
                f"Cannot restore: '{key}' has {len(versions)} version(s). "
                f"Need at least 2."
            )
            return False

        previous = versions[1]
        previous_version_id = previous["VersionId"]

        aws_s3_client.copy_object(
            Bucket=bucket_name,
            Key=key,
            CopySource={
                "Bucket": bucket_name,
                "Key": key,
                "VersionId": previous_version_id,
            },
        )
        print(
            f"Restored previous version ({previous_version_id}) of '{key}' "
            f"as the new latest version."
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False

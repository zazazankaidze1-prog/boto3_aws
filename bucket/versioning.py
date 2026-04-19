import logging
from botocore.exceptions import ClientError


def get_bucket_versioning_status(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get("Status")
        if status is None:
            return "Disabled"
        return status
    except ClientError as e:
        logging.error(e)
        return None


def enable_bucket_versioning(aws_s3_client, bucket_name):
    try:
        aws_s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False


def suspend_bucket_versioning(aws_s3_client, bucket_name):
    try:
        aws_s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Suspended"},
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False

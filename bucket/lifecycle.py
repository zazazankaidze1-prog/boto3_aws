import logging
from botocore.exceptions import ClientError


def set_lifecycle_policy(aws_s3_client, bucket_name, days=120):
    lifecycle_configuration = {
        "Rules": [
            {
                "ID": f"DeleteObjectsAfter{days}Days",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {"Days": days},
            }
        ]
    }

    try:
        aws_s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration,
        )
        print(f"Lifecycle policy set: objects will expire after {days} days")
        return True
    except ClientError as e:
        logging.error(e)
        return False


def get_lifecycle_policy(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return response.get("Rules", [])
    except ClientError as e:
        logging.error(e)
        return None


def delete_lifecycle_policy(aws_s3_client, bucket_name):
    try:
        aws_s3_client.delete_bucket_lifecycle(Bucket=bucket_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False

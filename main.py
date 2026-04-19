import argparse
import logging

from botocore.exceptions import ClientError

from auth import init_client
from bucket.crud import bucket_exists, create_bucket, delete_bucket, list_buckets
from bucket.encryption import read_bucket_encryption, set_bucket_encryption
from bucket.lifecycle import (
    delete_lifecycle_policy,
    get_lifecycle_policy,
    set_lifecycle_policy,
)
from bucket.policy import assign_policy, read_bucket_policy
from object.crud import (
    download_file_and_upload_to_s3,
    get_objects,
    upload_file,
    upload_large_file,
    upload_small_file,
)


parser = argparse.ArgumentParser(
    description="CLI program that helps with S3 buckets.",
    usage="""
    List buckets:
        python main.py -lb

    Create bucket:
        python main.py -bn my-bucket -cb -region us-east-1

    Upload SMALL file (<100MB) with mimetype validation:
        python main.py -bn my-bucket -usf -fp ./photo.jpg

    Upload LARGE file (multipart):
        python main.py -bn my-bucket -ulf -fp ./video.mp4

    Set 120-day lifecycle policy:
        python main.py -bn my-bucket -slc

    Get lifecycle policy:
        python main.py -bn my-bucket -gslc

    Delete lifecycle policy:
        python main.py -bn my-bucket -dlc
    """,
    prog="main.py",
    epilog="DEMO APP FOR BTU_AWS — Lecture 3",
)

# ---------- Bucket flags ----------
parser.add_argument("-lb", "--list_buckets", action="store_true", help="List buckets")
parser.add_argument("-cb", "--create_bucket", action="store_true", help="Create bucket")
parser.add_argument("-db", "--delete_bucket", action="store_true", help="Delete bucket")
parser.add_argument("-be", "--bucket_exists", action="store_true", help="Check if bucket exists")
parser.add_argument("-bn", "--bucket_name", type=str, help="Bucket name", default=None)
parser.add_argument("-region", "--region", type=str, help="Region", default="us-east-1")

# ---------- Bucket policy / encryption ----------
parser.add_argument("-rp", "--read_policy", action="store_true", help="Read bucket policy")
parser.add_argument("-arp", "--assign_read_policy", action="store_true", help="Assign public-read policy")
parser.add_argument("-amp", "--assign_missing_policy", action="store_true", help="Assign multi-action policy")
parser.add_argument("-ben", "--bucket_encryption", action="store_true", help="Enable AES256 encryption")
parser.add_argument("-rben", "--read_bucket_encryption", action="store_true", help="Read bucket encryption")

# ---------- Object flags ----------
parser.add_argument("-lo", "--list_objects", action="store_true", help="List objects in bucket")
parser.add_argument("-du", "--download_upload", action="store_true", help="Download from URL and upload to bucket")
parser.add_argument("-ol", "--object_link", type=str, help="URL to download and re-upload", default=None)

# ---------- NEW: small / large file upload ----------
parser.add_argument("-usf", "--upload_small_file", action="store_true", help="Upload small file (<100MB)")
parser.add_argument("-ulf", "--upload_large_file", action="store_true", help="Upload large file (multipart)")
parser.add_argument("-fp", "--file_path", type=str, help="Path to the file to upload", default=None)
parser.add_argument("-nomime", "--skip_mime_validation", action="store_true", help="Skip mimetype validation")

# ---------- NEW: lifecycle policy ----------
parser.add_argument("-slc", "--set_lifecycle", action="store_true", help="Set lifecycle policy (default 120 days)")
parser.add_argument("-gslc", "--get_lifecycle", action="store_true", help="Get lifecycle policy")
parser.add_argument("-dlc", "--delete_lifecycle", action="store_true", help="Delete lifecycle policy")
parser.add_argument("-days", "--lifecycle_days", type=int, help="Expiration days (default 120)", default=120)

# Legacy
parser.add_argument("-uf", "--upload_file", action="store_true", help="Simple upload (legacy)")


def main():
    s3_client = init_client()
    args = parser.parse_args()

    if args.list_buckets:
        buckets = list_buckets(s3_client)
        if buckets:
            print("Buckets:")
            for bucket in buckets["Buckets"]:
                print(f"  {bucket['Name']}")
        return

    if not args.bucket_name:
        parser.error("Most operations require --bucket_name / -bn")

    bn = args.bucket_name

    # -------- Bucket CRUD --------
    if args.create_bucket:
        if bucket_exists(s3_client, bn):
            parser.error(f"Bucket '{bn}' already exists")
        if create_bucket(s3_client, bn, args.region):
            print(f"Bucket '{bn}' created in {args.region}")

    if args.delete_bucket and delete_bucket(s3_client, bn):
        print(f"Bucket '{bn}' deleted")

    if args.bucket_exists:
        print(f"Bucket exists: {bucket_exists(s3_client, bn)}")

    # -------- Policies --------
    if args.read_policy:
        print(read_bucket_policy(s3_client, bn))
    if args.assign_read_policy:
        assign_policy(s3_client, "public_read_policy", bn)
    if args.assign_missing_policy:
        assign_policy(s3_client, "multiple_policy", bn)

    # -------- Encryption --------
    if args.bucket_encryption and set_bucket_encryption(s3_client, bn):
        print("Encryption enabled (AES256)")
    if args.read_bucket_encryption:
        print(read_bucket_encryption(s3_client, bn))

    # -------- Objects --------
    if args.list_objects:
        get_objects(s3_client, bn)

    if args.object_link and args.download_upload:
        print(download_file_and_upload_to_s3(s3_client, bn, args.object_link))

    validate_mime = not args.skip_mime_validation

    if args.upload_small_file:
        if not args.file_path:
            parser.error("--upload_small_file requires --file_path / -fp")
        upload_small_file(s3_client, bn, args.file_path, validate_mime=validate_mime)

    if args.upload_large_file:
        if not args.file_path:
            parser.error("--upload_large_file requires --file_path / -fp")
        upload_large_file(s3_client, bn, args.file_path, validate_mime=validate_mime)

    if args.upload_file:
        if not args.file_path:
            parser.error("--upload_file requires --file_path / -fp")
        upload_file(s3_client, args.file_path, bn)

    # -------- Lifecycle --------
    if args.set_lifecycle:
        set_lifecycle_policy(s3_client, bn, days=args.lifecycle_days)

    if args.get_lifecycle:
        rules = get_lifecycle_policy(s3_client, bn)
        if rules:
            print("Lifecycle rules:")
            for rule in rules:
                print(f"  {rule}")
        else:
            print("No lifecycle configuration set.")

    if args.delete_lifecycle and delete_lifecycle_policy(s3_client, bn):
        print("Lifecycle policy deleted")


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        logging.error(e)

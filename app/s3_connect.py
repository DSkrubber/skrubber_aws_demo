import json
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv

from app.loggers import logger

load_dotenv(find_dotenv())
BUCKET = os.environ.get("BUCKET")
REGION_NAME = os.environ.get("REGION_NAME")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


def connect_s3() -> boto3.resource:
    s3_resource = boto3.resource(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    logger.info(f"S3 resource {s3_resource} created")
    try:
        s3_resource.meta.client.head_bucket(Bucket=BUCKET)
        logger.info(f"Found bucket {BUCKET}")
    except ClientError:
        logger.info(f"No such bucket {BUCKET}. Creating...")
        create_bucket(s3_resource, BUCKET, region=REGION_NAME)
        logger.info(f"Bucket {BUCKET} created successfully")
    return s3_resource


def create_bucket(s3: boto3.resource, bucket_name: str, region: str):
    try:
        location = {"LocationConstraint": region}
        bucket = s3.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location
        )
    except ClientError:
        logger.info(f"Failed create bucket {BUCKET}")
        raise
    return bucket


def put_in_bucket(s3: boto3.resource, bucket: str, user_id: int) -> None:
    path_to_obj = f"{user_id}.json"
    json_obj = json.dumps({"user_id": user_id, "user_role": "s3_role"})
    s3.Bucket(bucket).put_object(Body=json_obj, Key=path_to_obj)
    logger.info(f"Create user {user_id} data in bucket {bucket}")


def get_bucket_object(s3: boto3.resource, bucket: str, user_id: int) -> dict:
    path_to_obj = f"{user_id}.json"
    obj = s3.Object(bucket, path_to_obj)
    user = obj.get()["Body"].read().decode("utf-8")
    user_data = json.loads(user)
    logger.info(f"Get user {user_id} data in bucket {bucket}")
    return user_data

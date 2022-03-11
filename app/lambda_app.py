import json
import os
import time

import boto3
import rediscluster
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv

from app.loggers import logger

load_dotenv(find_dotenv())

ELASTICACHE_CONFIG_ENDPOINT = os.environ.get("ELASTICACHE_CONFIG_ENDPOINT")
REGION_NAME = os.environ.get("REGION_NAME")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

LAMBDA_URL = os.environ.get("LAMBDA_URL")
LAMBDA_NAME = os.environ.get("LAMBDA_NAME")
LAMBDA_IAM_ROLE = os.environ.get("LAMBDA_IAM_ROLE")
LAMBDA_SUBNET_ID = os.environ.get("LAMBDA_SUBNET_ID")
LAMBDA_SECURITY_GROUP = os.environ.get("LAMBDA_SECURITY_GROUP")


def connect_elasticache() -> rediscluster.RedisCluster:
    url, port = ELASTICACHE_CONFIG_ENDPOINT.split(":")
    redis_cli = rediscluster.RedisCluster(
        startup_nodes=[{"host": url, "port": port}],
        decode_responses=True,
        skip_full_coverage_check=True,
    )
    if redis_cli and redis_cli.ping():
        logger.info(f"Connected to redis resource: {redis_cli}")
    return redis_cli


def connect_lambda() -> boto3.client:
    lambda_cli = boto3.client(
        "lambda",
        endpoint_url=LAMBDA_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    logger.info(f"Lambda resource {lambda_cli} created")
    create_lambda(lambda_cli, LAMBDA_NAME)
    logger.info(f"Initializing lambda function {LAMBDA_NAME}...")
    for attempts in range(180):
        try:
            lambda_cli.invoke(
                FunctionName=LAMBDA_NAME,
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps({"user_id": "test"}),
            )
            logger.info(f"Function {LAMBDA_NAME} initialized")
            break
        except ClientError:
            time.sleep(1)
    else:
        raise ClientError("Failed to initialize lambda", "lambda_init")
    return lambda_cli


def get_zip_bytes(path_to_archive: str) -> bytes:
    """Helper function to load zip file with lambda application as bytes."""
    with open(path_to_archive, "rb") as archive:
        return archive.read()


def create_lambda(lambda_cli: boto3.client, lambda_name: str):
    try:
        lambda_cli.create_function(
            FunctionName=lambda_name,
            Runtime="python3.9",
            Role=LAMBDA_IAM_ROLE,
            Handler="handler.func",
            Code={"ZipFile": get_zip_bytes("./app/application.zip")},
            VpcConfig={
                "SubnetIds": [LAMBDA_SUBNET_ID],
                "SecurityGroupIds": [LAMBDA_SECURITY_GROUP],
            },
            PackageType="Zip",
        )
        logger.info(f"Lambda function {lambda_name} created.")
    except ClientError:
        logger.info(f"Function {lambda_name} already exist. Updating")
        update_lambda(lambda_cli)


def update_lambda(lambda_cli):
    """Use to update lambda code."""
    response = lambda_cli.update_function_code(
        FunctionName=LAMBDA_NAME,
        ZipFile=get_zip_bytes("./app/application.zip"),
    )
    return response


def lambda_trigger(user_id):
    cache_client = connect_elasticache()
    cached_result = cache_client.get(user_id)
    if cached_result:
        logger.info(f"Get value for {user_id} from cache: {cached_result}")
        return cached_result
    logger.info(f"Value for {user_id} wasn't cached. Using Lambda...")
    payload = json.dumps({"user_id": user_id}).encode("utf-8")
    lambda_cli = connect_lambda()
    response = lambda_cli.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=payload,
    )
    lambda_result = json.loads(response["Payload"].read())
    logger.info(f"Get value for {user_id} from Lambda: {lambda_result}")
    cache_client.set(user_id, lambda_result)
    logger.info(f"Save value for {user_id} in cache")
    return lambda_result

import json
import os
import time
from typing import Tuple

import boto3
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv

from app.loggers import logger

load_dotenv(find_dotenv())

DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME")
DYNAMO_ENDPOINT_URL = os.environ.get("DYNAMO_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.environ.get("REGION_NAME")
SNS_ENDPOINT_URL = os.environ.get("SNS_ENDPOINT_URL")
SQS_ENDPOINT_URL = os.environ.get("SQS_ENDPOINT_URL")
SNS_TOPIC_NAME = os.environ.get("SNS_TOPIC_NAME")
SQS_QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME")
SQS_POLICY_LABEL = os.environ.get("SQS_POLICY_LABEL")


def connect_dynamodb_table() -> boto3.resource:
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMO_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    logger.info(f"DynamoDB resource {dynamodb} created")
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    try:
        table.load()
        logger.info(f"Loaded table {DYNAMO_TABLE_NAME}")
    except ClientError:
        logger.info(f"No such table {DYNAMO_TABLE_NAME}. Creating...")
        table = create_table(dynamodb, DYNAMO_TABLE_NAME)
        logger.info(f"Table {DYNAMO_TABLE_NAME} created successfully")
    return table


def create_table(dynamodb: boto3.resource, table_name: str):
    table = dynamodb.create_table(
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "N"},
            {"AttributeName": "user_role", "AttributeType": "S"},
        ],
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "user_role", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    )
    return table


def put_in_dynamodb(dynamodb: boto3.resource, table_name: str, user_id: int):
    dynamodb.meta.client.put_item(
        TableName=table_name,
        Item={
            "user_id": user_id,
            "user_role": "dynamo_db_role",
        },
        ReturnValues="ALL_OLD",
    )
    saved_object = get_dynamodb_object(dynamodb, table_name, user_id)
    logger.info(f"Create user data: {saved_object} in bucket {table_name}")


def get_dynamodb_object(dynamodb: boto3.resource, table: str, user_id: int):
    response = dynamodb.meta.client.query(
        TableName=table,
        Select="ALL_ATTRIBUTES",
        Limit=1,
        KeyConditionExpression="user_id = :user_id",
        ExpressionAttributeValues={":user_id": user_id},
    )
    return response["Items"][0]


def connect_sns() -> boto3.resource:
    sns = boto3.resource(
        "sns",
        endpoint_url=SNS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    logger.info(f"SNS resource {sns} created")
    return sns


def get_topic(sns: boto3.resource, topic_name: str) -> str:
    topic = sns.meta.client.create_topic(Name=topic_name)
    topic_arn = topic["TopicArn"]
    logger.info(f"SNS topic {topic_arn} created")
    return topic_arn


def publish_message(sns: boto3.resource, topic_name: str, user_id: int) -> str:
    topic_arn = get_topic(sns, topic_name)
    message = json.dumps({"user_id": user_id})
    sns.meta.client.publish(TopicArn=topic_arn, Message=message)
    logger.info(f"Success publish message {message} to sns topic {topic_arn}")
    return topic_arn


def connect_sqs() -> boto3.resource:
    sqs = boto3.resource(
        "sqs",
        endpoint_url=SQS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION_NAME,
    )
    logger.info(f"SQS resource {sqs} created")
    return sqs


def get_queue_url_arn(sqs: boto3.resource, queue_name: str) -> Tuple[str, str]:
    queue = sqs.Queue(queue_name)
    try:
        queue.load()
        logger.info(f"Loaded queue {queue_name}")
    except ClientError:
        logger.info(f"No such queue {queue_name}. Creating...")
        queue = sqs.create_queue(QueueName=queue_name)
        logger.info(f"Queue {queue_name} created successfully")
        logger.info(
            f"QueueUrl: {queue.url}, QueueArn: {queue.attributes['QueueArn']}"
        )
    return queue.url, queue.attributes["QueueArn"]


def subscribe_topic(sns: boto3.resource, topic_arn: str, sqs_arn: str) -> str:
    response = sns.meta.client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=sqs_arn,
    )
    logger.info(
        f"Successfully subscribe SQS (ARN {sqs_arn}) to SNS topic {topic_arn}"
    )
    return response["SubscriptionArn"]


def get_message(
    sqs: boto3.resource, topic_arn: str, sqs_arn: str, sqs_url: str
) -> dict:
    policy = {
        "Version": "2008-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
                "Sid": "__default_statement_ID",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["SQS:SendMessage"],
                "Resource": sqs_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
            },
        ],
    }
    sqs.meta.client.set_queue_attributes(
        QueueUrl=sqs_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    logger.info("Add permission for SNS to access SQS service")
    message_sns = sqs.meta.client.receive_message(
        QueueUrl=sqs_url,
        AttributeNames=["All"],
    )
    message_info = message_sns["Messages"][0]
    logger.info(
        f"Success receive message {message_info['MessageId']} "
        f"from SQS queue (URL {sqs_url})"
    )
    message_sqs = json.loads(message_info["Body"])["Message"]
    return json.loads(message_sqs)


def save_dynamo_queue(user_id: int) -> None:
    dynamodb = connect_dynamodb_table()
    sns, sqs = connect_sns(), connect_sqs()
    queue_url, queue_arn = get_queue_url_arn(sqs, SQS_QUEUE_NAME)
    topic_arn = get_topic(sns, SNS_TOPIC_NAME)
    subscribe_topic(sns, topic_arn, queue_arn)
    for attempts in range(3):
        try:
            publish_message(sns, SNS_TOPIC_NAME, user_id)
            user_queue_id = get_message(sqs, topic_arn, queue_arn, queue_url)
            break
        except (ClientError, KeyError):
            time.sleep(3)
            pass
    else:
        raise ClientError("Failed to receive message", "sqs_recieve")
    put_in_dynamodb(dynamodb, DYNAMO_TABLE_NAME, user_queue_id["user_id"])

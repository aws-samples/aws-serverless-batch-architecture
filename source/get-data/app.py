# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3
import os
import json
from botocore.exceptions import ClientError
from aws_lambda_powertools.utilities.validation import validate
from aws_lambda_powertools.utilities.validation.exceptions import SchemaValidationError
import schemas

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')


def lambda_handler(event, context):
    request = event.get('pathParameters')

    uuid = request.get('uuid')

    input_object = {"uuid": uuid}

    try:
        validate(event=input_object, schema=schemas.INPUT)
    except SchemaValidationError as e:
        return {"response": "failure", "error": e}

    table_name = os.environ['TABLE_NAME']

    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(
            Key={
                'uuid': request.get('uuid')
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']

    return {
        'statusCode': 200,
        'body': json.dumps({"item": item})
    }

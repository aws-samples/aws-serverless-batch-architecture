# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import boto3

s3_client = boto3.client('s3')
state_machine_client = boto3.client('stepfunctions')


def lambda_handler(event, context):
    bucket = event['bucket']
    state_machine_arn_filename = event['stateMachineArnFileName']

    response = s3_client.get_object(Bucket=bucket, Key=state_machine_arn_filename)
    file_data = response["Body"].read().decode('utf-8')
    json_data = json.loads(file_data)

    statuses = []

    for item in json_data:
        execution_response = state_machine_client.describe_execution(executionArn=item['executionArn'])
        status = execution_response["status"]
        statuses.append(status)

    if statuses.count("SUCCEEDED") == len(statuses):
        return "COMPLETED"
    elif "RUNNING" in statuses:
        return "PENDING"
    elif "FAILED" in statuses or "TIMED_OUT" in statuses or "ABORTED" in statuses:
        return "FAILED"

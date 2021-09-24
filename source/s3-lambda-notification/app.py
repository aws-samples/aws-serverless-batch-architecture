# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import os
import boto3
import time

state_machine_client = boto3.client('stepfunctions')


def lambda_handler(event, context):

    for record in event['Records']:
        param = {
            "Records": record,
            "inputArchiveFolder": os.environ['INPUT_ARCHIVE_FOLDER'],
            "fileChunkSize": int(os.environ['FILE_CHUNK_SIZE']),
            "fileDelimiter": os.environ['FILE_DELIMITER']

        }
        state_machine_arn = os.environ['STATE_MACHINE_ARN']
        state_machine_execution_name = os.environ['STATE_MACHINE_EXECUTION_NAME'] + str(time.time())

        response = state_machine_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=state_machine_execution_name,
            input=json.dumps(param)
        )

        print(response)

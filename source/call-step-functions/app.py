# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import json
import uuid
import boto3


state_machine_client = boto3.client('stepfunctions')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    input_file_part_names = event['splitFileNames']
    state_machine_arn = event['stateMachineArn']
    to_process_folder = event['toProcessFolder']
    output_path = to_process_folder.replace("to_process", "output")
    bucket = event['bucket']
    state_machine_execution_responses = []

    for filename in input_file_part_names:
        run_name = generate_run_name(filename)

        param = create_input(filename)

        response = state_machine_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=run_name,
            input=json.dumps(param)
        )

        state_machine_execution_responses.append(
            {"executionArn": response['executionArn'],
             "HTTPStatusCode": response['ResponseMetadata']['HTTPStatusCode']})


        s3_outputfile_key = output_path + "/_execution-data/state_machine_execution_responses.json"

        response = s3_client.put_object(Bucket=bucket,
                                        Key=s3_outputfile_key,
                                        Body=json.dumps(state_machine_execution_responses))

    return {"response": "success", "outputPath": s3_outputfile_key}


def generate_run_name(filename):
    last_part_pos = filename.rfind("/")
    if last_part_pos == -1:
        return ""
    filename_index = last_part_pos + 1
    input_file_name = filename[filename_index:]
    run_name = input_file_name + "-" + str(uuid.uuid4())

    return run_name


def create_input(filename):
    first_part_pos = filename.find("/")
    if first_part_pos == -1:
        return ""
    bucket_name = filename[:first_part_pos]
    file_prefix = filename[(first_part_pos + 1):]

    return {"input": {"bucket": bucket_name, "key": file_prefix}}

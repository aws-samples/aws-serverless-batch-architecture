# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import csv
import boto3

import json
from io import StringIO

s3_client = boto3.client('s3')

header = [
    'uuid',
    'country',
    'itemType',
    'salesChannel',
    'orderPriority',
    'orderDate',
    'region',
    'shipDate',
    'unitsSold',
    'unitPrice',
    'unitCost',
    'totalRevenue',
    'totalCost',
    'totalProfit'

]


def lambda_handler(event, context):
    dataset = event['enrichedData']
    input_file_key = event['FilePath']
    output_file_key = input_file_key.replace("to_process", "output")
    bucket_info = get_bucket_info(output_file_key)
    print(bucket_info)

    out_file = StringIO()
    file_writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)

    for data in dataset:
        if 'error-info' in data:
            continue
        data_list = convert_to_list(data)
        file_writer.writerow(data_list)

    response = s3_client.put_object(Bucket=bucket_info['bucket'],
                                    Key=bucket_info['key'],
                                    Body=out_file.getvalue())

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        message = 'Writing chunk to S3 failed' + json.dumps(response, indent=2)
        raise Exception(message)

    return {"response": "success"}


def convert_to_list(data):
    data_list = [data['uuid'], data['country'], data['itemType'], data['salesChannel'], data['orderPriority'],
                 data['orderDate'], data['region'], data['shipDate'],
                 data['financialdata']['item']['unitsSold'],
                 data['financialdata']['item']['unitPrice'],
                 data['financialdata']['item']['unitCost'],
                 data['financialdata']['item']['totalRevenue'],
                 data['financialdata']['item']['totalCost'],
                 data['financialdata']['item']['totalProfit']]

    return data_list

def get_bucket_info(filename):
    first_part_pos = filename.find("/")
    if first_part_pos == -1:
        return ""
    bucket_name = filename[:first_part_pos]
    file_prefix = filename[(first_part_pos + 1):]

    return {"bucket": bucket_name, "key": file_prefix}


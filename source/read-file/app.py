# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import csv
import s3fs
import os

s3 = s3fs.S3FileSystem(anon=False)

header = [
    'uuid',
    'country',
    'itemType',
    'salesChannel',
    'orderPriority',
    'orderDate',
    'region',
    'shipDate'
]


def lambda_handler(event, context):
    input_file = event['input']['FilePath']
    output_data = []
    skip_first = 0
    with s3.open(input_file, 'r', newline='', encoding='utf-8-sig') as inFile:
        file_reader = csv.reader(inFile)
        for row in file_reader:
            if skip_first == 0:
                skip_first = skip_first + 1
                continue
            new_object = {}
            for i in range(len(header)):
                new_object[header[i]] = row[i]

            output_data.append(new_object)

    return output_data




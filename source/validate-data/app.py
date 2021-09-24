# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from aws_lambda_powertools.utilities.validation import validate
from aws_lambda_powertools.utilities.validation.exceptions import SchemaValidationError
import schemas


def lambda_handler(event, context):
    try:
        validate(event=event, schema=schemas.INPUT)
    except SchemaValidationError as e:
        return {"response": "failure", "error": e}

    return {"response": "success"}

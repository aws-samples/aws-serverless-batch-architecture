# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
INPUT = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "$id": "http://example.com/example.json",
    "type": "object",
    "title": "Batch processing sample schema for the use case",
    "description": "The root schema comprises the entire JSON document.",
    "required": ["uuid"],
    "properties": {
        "uuid": {
            "type": "string",
            "maxLength": 9,
            "pattern": "[0-9]{9}"
        }
    },
}
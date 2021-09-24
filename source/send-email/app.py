# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    sender = event['sender']
    recipient = event['recipient']

    bucket = event['bucket']
    s3_output_file = event['s3OutputFileName']

    pre_signed_url = generate_s3_signed_url(bucket, s3_output_file)

    send_email(sender, recipient, pre_signed_url)

    return {"response": "success"}


def generate_s3_signed_url(bucket, s3_target_key):
    return s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': bucket,
                                                    'Key': s3_target_key},
                                            ExpiresIn=3600)


def send_email(sender, recipient, pre_signed_url):
    # The subject line for the email.
    subject = "Batch Processing complete: Output file information"

    # The email body for recipients with non-HTML email clients.
    body_text = ("The file has been processed successfully\r\n"
                 "Click the pre-signed S3 URL to access the output file "
                 + pre_signed_url + ", The link will expire in 60 minutes."
                 )

    # The HTML body of the email.
    body_html = """<html>
    <head></head>
    <body>
      <h1>The file has been processed successfully</h1>
      <p>Click the pre-signed S3 URL to access the output file:
        <a href='{url}'>Output File</a></p>
      <p>The link will expire in 60 minutes.</p>
    </body>
    </html>""".format(url=pre_signed_url)

    # The character encoding for the email.
    charset = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses')

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': charset,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': charset,
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
            },
            Source=sender,

        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

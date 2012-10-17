#!/usr/bin/python

import boto.sqs
import boto.sqs.jsonmessage
import sys

# To include 'aws_consts', which should not be part of the project.
sys.path.append('../')
import aws_consts

def construct_queue_arn(
    queue_name,
    account_id=aws_consts.ACCOUNT_ID,
    region_name=aws_consts.REGION_NAME):
    return 'arn:aws:sqs:%(region_name)s:%(account_id)s:%(queue_name)s' % {
        'region_name': region_name, 'account_id': account_id,
        'queue_name': queue_name}

sqs_conn = boto.sqs.connect_to_region(
    region_name=aws_consts.REGION_NAME,
    aws_access_key_id = aws_consts.ACCESS_KEY_ID,
    aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY)

queue_name = 'wait_for_job_complete_queue'
queue_arn = construct_queue_arn(queue_name)

queue = sqs_conn.get_queue(queue_name)
messages = queue.get_messages()
message = messages[0]

msg = boto.sqs.jsonmessage.JSONMessage(body=message.get_body())
print msg.decode(message.get_body())


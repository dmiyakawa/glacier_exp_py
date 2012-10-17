#!/usr/bin/python

import boto.glacier
import boto.glacier.layer2
import boto.sns
import boto.sqs
import datetime
import sys
import time

# To include 'aws_consts', which should not be part of the project.
sys.path.append('../')
import aws_consts

DEFAULT_SNS_TOPIC = 'wait_for_job_complete_topic'
DEFAULT_SQS_QUEUE = 'wait_for_job_complete_queue'

# See:
# - http://docs.amazonwebservices.com/sns/latest/gsg/SendMessageToSQS.html
# - http://www.elastician.com/2010/04/subscribing-sqs-queue-to-sns-topic.html
SQS_POLICY_TMPL = """\
{
  "Version": "2008-10-17",
  "Id": "%(queue_arn)s/SQSDefaultPolicy",
  "Statement": [
    {
      "Sid": "Sid1350464522515",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "SQS:SendMessage",
      "Resource": "%(queue_arn)s",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "%(topic_arn)s"
        }
      }
    }
  ]
}
"""

def construct_arn(service, name,
    account_id=aws_consts.ACCOUNT_ID,
    region_name=aws_consts.REGION_NAME):
    return 'arn:aws:%(service)s:%(region_name)s:%(account_id)s:%(name)s' % {
        'service': service,
        'region_name': region_name, 'account_id': account_id,
        'name': name}


def has_endpoint_for(topic_arn, queue_arn):
    ret_data = sns_conn.get_all_subscriptions_by_topic(topic_arn)
    response = ret_data['ListSubscriptionsByTopicResponse']
    subscriptions = response['ListSubscriptionsByTopicResult']['Subscriptions']
    for subscription in subscriptions:
        if subscription['Endpoint'] == queue_arn:
            return True;
        pass
    return False


def sns_topic_exists(sns_conn, topic_arn):
    try:
        attribute = sns_conn.get_topic_attributes(topic_arn)
    except:
        attribute = None
        pass
    return attribute is not None


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print >>sys.stderr, 'usage: %s vault_name' % sys.argv[0]
        sys.exit(1)
        pass
    vault_name = sys.argv[1]
    vault_arn  = construct_arn('glacier', vault_name)
    topic_name = DEFAULT_SNS_TOPIC
    topic_arn  = construct_arn('sns', topic_name)
    queue_name = DEFAULT_SQS_QUEUE
    queue_arn  = construct_arn('sqs', queue_name) 

    layer2 = boto.glacier.layer2.Layer2(
        aws_access_key_id = aws_consts.ACCESS_KEY_ID,
        aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY,
        region_name=aws_consts.REGION_NAME)
    try:
        vault = layer2.get_vault(vault_name)
    except boto.glacier.exceptions.UnexpectedHTTPResponseError, e:
        vault = None
        pass

    if not vault:
        print >>sys.stderr, 'Vault %s isn\'t available.' % vault_name
        sys.exit(1)
        pass
    
    sqs_conn = boto.sqs.connect_to_region(
        region_name=aws_consts.REGION_NAME,
        aws_access_key_id = aws_consts.ACCESS_KEY_ID,
        aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY)
    queue = sqs_conn.get_queue(queue_name)
    if queue is not None:
        if queue.count() > 0:
            print >>sys.stderr, 'Queue %s has message(s).' % queue_name
            print >>sys.stderr, 'Removing each item'
            messages = queue.get_messages(num_messages=queue.count())
            for i, message in enumerate(messages):
                print >>sys.stderr, '%d (type: %s): %s' % (
                    i, type(message), str(message))
                queue.delete_message(message)
                pass
            print >>sys.stderr, ''.join([
                'All existing messages should be marked as ',
                'deleted, but may be remaining for a while.\n',
                'Re-run this script after a while, or after ',
                'confirming everything is ready again.'])
            sys.exit(1)
            pass
        # TODO: Verify Policy validity of the existing queue.
        pass
    else:
        print >>sys.stderr, 'queue %s doesn\'t exist. Creating it.' % (
            queue_name)
        queue = sqs_conn.create_queue(queue_name)
        queue.set_attribute(
            'Policy',
            SQS_POLICY_TMPL % {'queue_arn': queue_arn,
                               'topic_arn': topic_arn})
        pass
    sns_conn = boto.sns.connect_to_region(
        region_name=aws_consts.REGION_NAME,
        aws_access_key_id = aws_consts.ACCESS_KEY_ID,
        aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY)
    if sns_topic_exists(sns_conn, topic_arn):
        if not has_endpoint_for(topic_arn, queue_arn):
            print >>sys.stderr, 'Topic %s doesn\'t has a endpoint for %s' % (
                topic_arn, queue_arn)
            sns_conn.subscribe(topic_arn, 'sqs', queue_arn)
            pass
        pass
    else:
        print >>sys.stderr, 'topic %s doesn\'t exist. Creating it.' % (
            topic_name)
        topic = sns_conn.create_topic(topic_name)
        sns_conn.subscribe(topic_arn, 'sqs', queue_arn)
        pass

    print 'Vault: %s' % vault_arn
    print 'Queue: %s' % queue_arn
    print 'Topic: %s' % topic_arn
    print

    layer2.layer1.set_vault_notifications(
        vault_name,
        {'SNSTopic': topic_arn,
         'Events': ['ArchiveRetrievalCompleted',
                    'InventoryRetrievalCompleted']})

    print 'Waiting for some message from the queue %s.' % queue_name

    t1 = time.time()
    while True:
        messages = queue.get_messages()
        if messages:
            message = messages[0]
            if type(message) is str:
                msg_content = message
            else:  # Trust it is a boto.sqs.message.Message kind.
                msg_content = message.get_body()
                pass
            print 'New message found: %s' % msg_content
            break
        else:
            # 1 min.
            time.sleep(60)
            pass
        pass
    t2 = time.time()

    print 'Waited for %f min.' % ((t2 - t1) / 60)
    print datetime.datetime.today().strftime('Now: %Y-%m-%d %H:%S')

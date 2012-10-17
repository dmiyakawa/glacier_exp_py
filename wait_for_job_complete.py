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

def construct_topic_arn(
    topic_name,
    account_id=aws_consts.ACCOUNT_ID,
    region_name=aws_consts.REGION_NAME):
    return 'arn:aws:sns:%(region_name)s:%(account_id)s:%(topic_name)s' % {
        'region_name': region_name, 'account_id': account_id,
        'topic_name': topic_name}


def construct_queue_arn(
    queue_name,
    account_id=aws_consts.ACCOUNT_ID,
    region_name=aws_consts.REGION_NAME):
    return 'arn:aws:sqs:%(region_name)s:%(account_id)s:%(queue_name)s' % {
        'region_name': region_name, 'account_id': account_id,
        'queue_name': queue_name}


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

    topic_name = DEFAULT_SNS_TOPIC
    topic_arn  = construct_topic_arn(topic_name)
    queue_name = DEFAULT_SQS_QUEUE
    queue_arn  = construct_queue_arn(queue_name)

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
    
    jobs = vault.list_jobs()
    if len(jobs) == 0:
        print >>sys.stderr, 'Job not available'
        sys.exit(1)
        pass

    if len(jobs) > 1:
        print >>sys.stderr, 'Multiple job id found:'
        for i, job in enumerate(jobs):
            print '%d: %s' % (i, job.id)
            pass
        # TODO: implement it :-P
        # print >>sys.stderr, 'Specify job_id using -j, orindex with -i'
        sys.exit(1)
        pass
    else:
        job = jobs[0]
        pass

    sqs_conn = boto.sqs.connect_to_region(
        region_name=aws_consts.REGION_NAME,
        aws_access_key_id = aws_consts.ACCESS_KEY_ID,
        aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY)
    queue = sqs_conn.get_queue(queue_name)
    if queue is not None:
        print >>sys.stderr, 'Queue %s already exists.' % queue_name
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
                'deleted, but may be remaining for a while. ',
                'Re-run this script after a while, or after ',
                'confirming everything is ready again.'])
            sys.exit(1)
            pass
        pass
    else:
        print >>sys.stderr, 'queue %s doesn\'t exist. Creating it.' % (
            queue_name)
        queue = sqs_conn.create_queue(queue_name)
        pass
    print 'Queue: ', queue

    sns_conn = boto.sns.connect_to_region(
        region_name=aws_consts.REGION_NAME,
        aws_access_key_id = aws_consts.ACCESS_KEY_ID,
        aws_secret_access_key = aws_consts.SECRET_ACCESS_KEY)
    if sns_topic_exists(sns_conn, topic_arn):
        print >>sys.stderr, 'Topic %s already exists.' % topic_name
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

    layer2.layer1.set_vault_notifications(
        vault_name,
        {'SNSTopic': topic_arn,
         'Events': ['ArchiveRetrievalCompleted',
                    'InventoryRetrievalCompleted']})

    print 'Now, waiting for some message from the queue %s.' % queue_name

    t1 = time.time()
    while True:
        messages = queue.get_messages()
        if messages:
            message = messages[0]
            print 'New message found (type: %s): %s' % (
                type(message), str(message))
            # queue.delete_message(message)
            break
        else:
            # 1 min.
            time.sleep(60)
            pass
        pass
    t2 = time.time()
    print 'Waited for %f min.' % ((t2 - t1) / 60)
    print datetime.datetime.today().strftime('Now: %Y-%m-%d %H:%S')

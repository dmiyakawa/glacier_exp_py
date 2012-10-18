#!/usr/bin/python
#
# Launch a very simple web server to handle notifications from Amazon SNS.
# The server can receive two kinds of notifications:
#
# - SubscriptionConfirmation
#
#   This will be sent when a user creates a new subscription for the topic to
#   this end point. "http://(host):(port)/" should be specified when the new
#   subscription is done. Without handling this notification, Amazon SNS
#   won't send any real notification to this server :-(
#
# - Notification
#   Actual notification.
#
# See http://docs.amazonwebservices.com/sns/latest/gsg/SendMessageToHttp.html

import BaseHTTPServer
import cgi
import cgitb
import json
import logging
import sys
import urllib
import xml.etree.ElementTree

cgitb.enable()
logging.basicConfig(level=logging.INFO)

known_topic_arns = set([])
subscription_arns = set([])

""" TODO: document :-P """ 
class SNSHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        BaseHTTPServer.BaseHTTPRequestHandler.__init__(
            self, request, client_address, server)
        pass

    def do_GET(self):
        logging.debug('GET request to "%s". Ignoring.' % self.path)
        self.send_error(404, 'File Not Found (%s) :-P' % self.path)
        pass

    def do_POST(self):
        logging.debug('POST request to "%s".' % self.path)
        logging.debug('Header:\n%s' % self.headers)

        content_list = []
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write('<html><head><title>Hello</title></head>')
        self.wfile.write('<body>Hello</body></html>\n')

        content = self.rfile.read()

        logging.debug('Content:\n%s' % content)
        json_obj = json.loads(content)
        notification_type = json_obj['Type']
        topic_arn = json_obj['TopicArn']

        logging.debug('Type: %s' % notification_type)
        if notification_type == 'SubscriptionConfirmation':
            subscribe_url = json_obj['SubscribeURL']
            logging.debug('SubscribeURL: %s' % subscribe_url)

            if topic_arn not in known_topic_arns:
                logging.info('Remembering Topic ARN "%s"' % topic_arn)
                # Remember the topic ARN to accept notifications from
                known_topic_arns.add(topic_arn)
                pass

            # Actualy subscribe the sns topic by accessing to the site.
            f = urllib.urlopen(subscribe_url)
            response_str = f.read()
            logging.debug(response_str)
            tree = xml.etree.ElementTree.fromstring(response_str)
            # getiterator() is obsolete in 2.7 but available in 2.6
            for elem in tree.getiterator():
                if 'SubscriptionArn' in elem.tag:
                    arn = elem.text
                    logging.info('New arn "%s" is subscribed.' % arn)
                    subscription_arns.add(arn)
                    break
                pass
            pass
        elif notification_type == 'Notification':
            subject = json_obj['Subject']
            message = json_obj['Message']
            logging.info('Subject: %s' % subject)
            logging.info('Message: %s' % message)

            # TODO: check x-amz-sns-subscription-arn in the header to check
            # the validity of the subscription
            if topic_arn not in known_topic_arns:
                logging.warn('Unknown topic ARN "%s"' % topic_arn)
                pass
            pass

        pass
    pass

if __name__ == '__main__':
    try:
        server = BaseHTTPServer.HTTPServer(('', 18081), SNSHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()
        pass
    pass

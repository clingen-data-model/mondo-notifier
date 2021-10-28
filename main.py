import os

from flask import escape
from google.cloud import pubsub_v1


def publish_message(topic_name, msg):
    publisher = pubsub_v1.PublisherClient()
    future = publisher.publish(topic_name, msg)
    future.result()


def pubsub_queue():
    return os.environ.get('PUBSUB_QUEUE')


def mondo_notifier(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
    elif request_args and 'name' in request_args:
        name = request_args['name']
    else:
        name = 'World'

    topic_name = pubsub_queue()
    publish_message(topic_name, b'{"message": "Hello, World!"}')

    return 'Hello {}! Our pubsub destination is called {}'.format(
        escape(name), escape(topic_name))

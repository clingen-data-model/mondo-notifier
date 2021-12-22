import os
import json
import hmac
import hashlib
import google.cloud.logging
import logging
import jsonschema
import requests
import pandas as pd
import io

from flask import escape
from google.cloud import pubsub_v1
from jsonschema import validate

client = google.cloud.logging.Client()
client.setup_logging()

tsv_files = ['mondo_obsoletioncandidates.tsv', 'mondo_release_diff_changed_terms.tsv', 'mondo_release_diff_new_terms.tsv']

# Describe what kind of json you expect.
releasePayloadSchema = {
  "type": "object",
  "properties": {
    "action": {"type": "string"},
    "release": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "draft": {"type": "boolean"},
        "prerelease": {"type": "boolean"},
        "created_at": {"type": "string", "format": "date-time"},
        "published_at": {"type": "string", "format": "date-time"},
        "assets": {
          "type": "array",
          "items": {
            "type":"object",
            "properties": {
              "name": {"type": "string"},
              "browser_download_url": {"type": "string"}
            },
            "required": ["name","browser_download_url"]
          }
        }
      },
      "required": ["name","draft","prerelease","published_at","assets"]
    },
    "repository": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "full_name": {"type": "string"}
      },
      "required": ["name","full_name"]
    }
  },
  "required": ["action","release","repository"]
}

def validate_signature(request):

    # logging.info(json.dumps(dict(request.headers), indent=4))
    signature_header = request.headers.get('X-Hub-Signature-256')
    sha_name, github_signature = signature_header.split('=')
    if sha_name != 'sha256':
      logging.error('X-Hub-Signature in payload headers was not sha256=****')
      return False

    # Create our own signature
    secret = os.environ.get('WEBHOOK_SECRET')
    local_signature = hmac.new(secret.encode('utf-8'), msg=request.data, digestmod=hashlib.sha256)

    # See if they match
    return hmac.compare_digest(local_signature.hexdigest(), github_signature)

def validateJson(jsonData):
    try:
      validate(instance=jsonData, schema=releasePayloadSchema)
    except jsonschema.exceptions.ValidationError as err:
      logging.error("Payload schema error: '{}'".format(err))
      return False
    return True

def publish_message(pubsub_topic, data, dest_topic, name, published_at):
    publisher = pubsub_v1.PublisherClient()
    future = publisher.publish(pubsub_topic, data=data, dest_topic=dest_topic, name=name, published_at=published_at )
    future.result()

def pubsub_queue():
    return os.environ.get('PUBSUB_MONDO_NOTIFY')

def process_release(release_json):
    name = release_json["name"]
    published_at = release_json["published_at"]

    # validate the tsv assets exist
    assets = release_json["assets"]
    token = os.environ.get('GITHUB_TOKEN')

    # TODO log warning/info if all 3 expected files are not found

    # TODO consider checking that the file column names do not change over time
    # - if we add schemas to the kafka topics it should fail there!

    #process files (validate, parse, publish)
    pubsub_topic = pubsub_queue()
    filtered_assets = (a for a in assets if a['name'].lower() in tsv_files)

    for asset in filtered_assets:
        # get/read file, send to parser
        logging.info("Reading browser_download_url '{}'".format(asset['browser_download_url']))

        r = requests.get(asset['browser_download_url'], headers={'Authorization': f'token {token}'}, params={})

        # Reading the downloaded content and turning it into a pandas dataframe
        df = pd.read_csv(io.StringIO(r.text), sep='\t')
        # logging.info('\t'+ df.head(5).to_string().replace('\n', '\n\t'))

        messages = df.to_dict (orient='records')

        # tsv filename is the destination topic
        dest_kafka_topic = asset['name'].split('.')[0]
        for m in messages:
            m_json = json.dumps(m)
            publish_message(pubsub_topic, data=m_json.encode('utf8'), dest_topic=dest_kafka_topic.encode('utf8'), name=name.encode('utf8'), published_at=published_at.encode('utf8'))

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

    if not request:
      logging.error("No request object.")
      return 'No request object. Processing stopped.'

    if not validate_signature(request):
      logging.error("Github signature did not validate.")
      return 'Webhook signature did not validate. Processing stopped.'

    request_json = request.get_json(silent=True)
    if not request_json:
      logging.error("No json payload in request.")
      return 'No json payload in request. Processing stopped.'

    if "action" not in request_json:
      logging.error("Json request did not contain expected 'action' key.")
      return "Json request did not contain expected 'action' key. Processing stopped."

    action = request_json['action']
    if action != "released":
      return "Not a 'action=released' request. Ignore request."

    if not validateJson(request_json):
      logging.info(request_json)
      return "Request json did not validate against released payload schema. Processing stopped."

    logging.info("Action is '{}'. Starting to process files.".format(action))
    process_release(request_json["release"])

    return 'Release {}! Processed successfully.'.format(escape(request_json["release"]["name"]))

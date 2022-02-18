import os
import json
import hmac
import hashlib
import google.cloud.logging
import logging
import jsonschema
import requests
import pandas as pd
import numpy as np
import io

from flask import escape
from jsonschema import validate
from confluent_kafka import Producer, KafkaError

delivered_records = 0

client = google.cloud.logging.Client()
client.setup_logging()

# Describe what kind of json you expect.
releasePayloadSchema = {
  "type": "object",
  "properties": {
    "action": {"type": "string"},
    "release": {
      "type": "object",
      "properties": {
        "tag_name": {"type": "string"},
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
    git_webhook_secret = os.environ.get('GIT_MONDO_WEBHOOK_SECRET')
    local_signature = hmac.new(git_webhook_secret.encode('utf-8'), msg=request.data, digestmod=hashlib.sha256)

    # See if they match
    return hmac.compare_digest(local_signature.hexdigest(), github_signature)

def validateJson(jsonData):
    try:
      validate(instance=jsonData, schema=releasePayloadSchema)
    except jsonschema.exceptions.ValidationError as err:
      logging.error("Payload schema error: '{}'".format(err))
      return False
    return True

def init_event_configs(release_json):
    event_cfgs_str = os.environ.get('MONDO_RELEASE_EVENTS')
    event_cfgs = json.loads(event_cfgs_str)
    # pprint(event_cfgs)

    for event_cfg in event_cfgs:
        for assets in release_json['assets']:
            if (assets['name'].lower() in event_cfg["asset_name"]):
                y = {
                    "browser_download_url": assets['browser_download_url'],
                    "release_name": release_json['name'],
                    "release_tag": release_json['tag_name'],
                    "release_date": release_json['published_at']}
                event_cfg.update(y)
                # pprint(event_cfg)

    return event_cfgs

def get_event_records(event_cfg):
    r = requests.get(event_cfg['browser_download_url'])
    # pprint(r.status_code)
    r.encoding = 'utf-8'
    # pprint(r.encoding)
    content = r.text

    # Reading the downloaded content and turning it into a pandas dataframe
    df = pd.read_csv(io.StringIO(content), sep='\t').fillna(np.nan).replace([np.nan], [None])
    # print('\t'+ df.head(5).to_string().replace('\n', '\n\t'))

    # if this is true then there are duplicates for this set (log warning that only the first is kept)
    if df.duplicated(subset=event_cfg['pk']).agg(max):
        logging.warning("Duplicate records in "+event_cfg['asset_name'])
        # print("Duplicate records in "+event_cfg['asset_name'])
        # keeps only the first unique row for each group.
        df = df.groupby(event_cfg['pk']).head(1)

    return df.to_dict (orient='records')

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        logging.error("Failed to deliver message: {}".format(err))
        # print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        # print("Produced record to topic {} partition [{}] @ offset {}"
        #       .format(msg.topic(), msg.partition(), msg.offset()))

def publish_events(producer, event_cfg, event_recs):
    global delivered_records
    delivered_records = 0

    for rec in event_recs:
        msg = {
            "release_name": event_cfg['release_name'],
            "release_date": event_cfg['release_date'],
            "event_type": event_cfg['name'],
            "content": rec}
        record_value = json.dumps(msg).encode('utf8')
        # logging.info("Producing record: {}".format(record_value))
        producer.produce(event_cfg['topic'], value=record_value, on_delivery=acked)
        # # p.poll() serves delivery reports (on_delivery)
        # # from previous produce() calls.
        producer.poll(0)

    producer.flush()

    logging.info("{} '{}' messages were produced to topic {}!".format(delivered_records, event_cfg['name'], event_cfg['topic']))
    # print("{} messages were produced to topic {}!".format(delivered_records, event_cfg['topic']))

def process_release(release_json):
    dx_servers = os.environ.get('DX_BOOTSTRAP_SERVERS')
    dx_producer_creds_str = os.environ.get('DX_MONDO_PRODUCER_CREDENTIALS')
    dx_producer_creds = json.loads(dx_producer_creds_str)

    conf = {
        "bootstrap.servers" : dx_servers,
        "security.protocol" : "SASL_SSL",
        "sasl.mechanisms" : "PLAIN",
        "sasl.username" : dx_producer_creds["user"],
        "sasl.password" : dx_producer_creds["password"]
    }
    producer = Producer(conf)

    events_cfg = init_event_configs(release_json)

    for event_cfg in events_cfg:
        event_recs = get_event_records(event_cfg)
        publish_events(producer, event_cfg, event_recs)

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
      return ('No request object. Processing stopped.', 400)

    if not validate_signature(request):
      logging.error("Github signature did not validate.")
      return ('Webhook signature did not validate. Processing stopped.', 401)

    request_json = request.get_json(silent=True)
    if not request_json:
      logging.error("No json payload in request.")
      return ('No json payload in request. Processing stopped.', 400)

    if "action" not in request_json:
      logging.error("Json request did not contain expected 'action' key.")
      return ("Json request did not contain expected 'action' key. Processing stopped.", 400)

    action = request_json['action']
    if action != "released":
      return ("Not a 'action=released' request. Ignore request.", 200)

    if not validateJson(request_json):
      logging.info(request_json)
      return ("Request json did not validate against released payload schema. Processing stopped.", 400)

    logging.info("Action is '{}'. Starting to process files.".format(action))
    process_release(request_json["release"])

    return ('Release {}! Processed successfully.'.format(escape(request_json["release"]["name"])), 200)

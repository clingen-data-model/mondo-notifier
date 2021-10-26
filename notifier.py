import requests
import json


def mondo_notifier(event, context):
    resp = requests.get(
        'https://api.github.com/repos/monarch-initiative/mondo/releases/latest'
    )
    release_tag = {'release': resp.json()['tag_name']}
    print(json.dumps(release_tag))


if __name__ == '__main__':
    mondo_notifier(None, None)

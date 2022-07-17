import os
from json import dumps
from urllib.request import Request, urlopen
from urllib.error import HTTPError


GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]


def github_request(method: str, api: str, json=None, timeout=10):
    url = os.path.join('https://api.github.com', api)
    # make request
    request = Request(url, method=method, headers={
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'token {GITHUB_TOKEN}',
    })
    # add json body
    data = None
    if json is not None:
        data = dumps(json).encode(('utf-8'))
        request.add_header('Content-Type', 'application/json; charset=utf-8')
        request.add_header('Content-Length', len(data))
    # make request
    return urlopen(request, data, timeout=timeout)


def lambda_handler(event, context):
    try:
        response = github_request(method='POST', api='repos/nmichlo/mscluster-status/actions/workflows/poll-cluster.yml/dispatches', json={'ref': 'main'})
        return {
            'statusCode': response.code,
            'body': str(response.reason)
        }
    except HTTPError as e:
        return {
            'statusCode': e.code,
            'body': str(e)
        }

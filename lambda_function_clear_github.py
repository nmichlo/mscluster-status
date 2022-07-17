import os
import json
from http.client import HTTPResponse
from urllib.request import Request, urlopen
from urllib.error import HTTPError


GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]


def github_request(method: str, api: str, json_data=None, timeout=15, json_response=False):
    url = f'https://api.github.com/{api.lstrip("/")}'
    # make request
    request = Request(url, method=method, headers={
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'token {GITHUB_TOKEN}',
    })
    # add json body
    data = None
    if json_data is not None:
        data = json.dumps(json_data).encode(('utf-8'))
        request.add_header('Content-Type', 'application/json; charset=utf-8')
        request.add_header('Content-Length', len(data))
    # make request
    response: HTTPResponse = urlopen(request, data, timeout=timeout)
    # get response json
    if json_response:
        dat = json.loads(response.fp.read())
        return response, dat
    return response


RUNS_PER_PAGE = 100


def _load_runs_page(page: int, allow_error=False) -> (int, list):
    try:
        response, dat = github_request(
            method='GET',
            api=f'repos/nmichlo/mscluster-status/actions/runs?per_page={RUNS_PER_PAGE}&page={page}',
            json_response=True,
        )
    except HTTPError as e:
        print(f'failed to retrieve runs page: {page}, error: {e}')
        if allow_error:
            return 0, []
        raise e
    # extract data
    total = dat['total_count']
    run_ids = [run['id'] for run in dat['workflow_runs']]
    # log everything
    print(f'loaded: {len(run_ids)} runs for page: {page} of {(total+RUNS_PER_PAGE-1)//RUNS_PER_PAGE}')
    return total, run_ids


def _try_delete_run(id: int):
    try:
        github_request(method='DELETE', api=f'/repos/nmichlo/mscluster-status/actions/runs/{id}')
        return id
    except HTTPError as e:
        print(f'failed to delete run: {id}, error: {e}')
        return None


def lambda_handler(event, context):
    deleted_runs = []
    try:
        # load the first page of runs
        total, runs = _load_runs_page(page=1)
        all_runs = list(runs)

        # collect the remaining runs
        for i in range(2, (total+RUNS_PER_PAGE-1)//RUNS_PER_PAGE + 1):
            _, runs = _load_runs_page(page=i, allow_error=True)
            all_runs.extend(runs)

        # delete all but the 10 newest runs
        remove_runs = all_runs[10:][::-1]
        for i, id in enumerate(remove_runs):
            print(f'deleting: {id}, {i+1}/{len(remove_runs)}')
            deleted_id = _try_delete_run(id=id)
            if deleted_id is not None:
                deleted_runs.append(deleted_id)

        return {
            'StatusCode': 200,
            'deleted_num': len(deleted_runs),
            'deleted_runs': json.dumps(deleted_runs)
        }
    except Exception as e:
        return {
            'StatusCode': 500,
            'error': str(e),
            'deleted_num': len(deleted_runs),
            'deleted_runs': json.dumps(deleted_runs)
        }

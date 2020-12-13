import json
from flask import make_response

# Currently not being used, flask-cors is used instead
def output_json(data, code, headers=None):
    dumped = json.dumps(data)
    if headers:
        headers.update({'Content-Type': 'application/json'})
        headers.update({'Access-Control-Allow-Origin': '*'})
        headers.update({'Access-Control-Allow-Headers': 'Content-Type,Authorization'})
        headers.update({'Access-Control-Allow-Methods': 'GET,POST'})
    else:
        headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST',
        }
    response = make_response(dumped, code, headers)
    return response
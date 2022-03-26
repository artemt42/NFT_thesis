# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 15:59:03 2022

@author: atishakov
"""


import requests
import os
import json
import time

def api_request():
    app_id = os.getenv('reddit_app_id')
    secret = os.getenv('reddit_app_secret')
    auth = requests.auth.HTTPBasicAuth(app_id, secret)
    reddit_username = os.getenv('reddit_username')
    reddit_password = os.getenv('reddit_password')
    data = {
            'grant_type': 'password',
            'username': reddit_username,
            'password': reddit_password
            }
    headers = {'User-Agent': 'NFT_Thesis/0.0.1'}
    res = requests.post('https://www.reddit.com/api/v1/access_token',
    auth=auth, data=data, headers=headers)
    api = json.loads(res.text)
    api['time_updated'] = int(round(time.time(),0))
    api = json.dumps(api)
    with open('api_keys/reddit_api.txt',"w") as f:
        f.write(api)

def request():
    with open('api_keys/reddit_api.txt',"r") as f:
        api_reddit = f.read()
    api_reddit = json.loads(api_reddit)
    curr_time = int(round(time.time(),0))
    api_req_time = api_reddit['time_updated']
    expires_in = api_reddit['expires_in']
    to_request = (curr_time - api_req_time) > expires_in
    if to_request:
        api_request()
        return "New key retrieval successful!"
    else:
        return "Using the same key..."

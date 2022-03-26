# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 16:43:03 2022

@author: atishakov
"""
import time
import json
import os
import requests

app_id = os.getenv('reddit_app_id')
secret = os.getenv('reddit_app_secret')
reddit_username = os.getenv('reddit_username')
reddit_password = os.getenv('reddit_password')
auth = requests.auth.HTTPBasicAuth(app_id, secret)
headers = {'User-Agent': 'NFT_Thesis/0.0.1'}
data = {
        'grant_type': 'password',
        'username': reddit_username,
        'password': reddit_password
        }

def js(d):
    return json.loads(d)

def api_file():
    with open('api_keys/reddit_api.txt',"r") as f:
        api_reddit = f.read()
    api_reddit = json.loads(api_reddit)
    return api_reddit

def api_request():
    res = requests.post('https://www.reddit.com/api/v1/access_token',
    auth=auth, data=data, headers=headers)
    api = json.loads(res.text)
    api['time_updated'] = int(round(time.time(),0))
    api = json.dumps(api)
    with open('api_keys/reddit_api.txt',"w") as f:
        f.write(api)

def api_key_check(force = False):
    api_reddit = api_file()
    curr_time = int(round(time.time(),0))
    api_req_time = api_reddit['time_updated']
    expires_in = api_reddit['expires_in']
    to_request = (curr_time - api_req_time) > expires_in
    if to_request or force:
        api_request()
        print("New key retrieval successful!")
        api_reddit = api_file()
    else:
        print("Using the same key...")
    return api_reddit['access_token']


token = api_key_check(force = True)
headers['Authorization'] = 'bearer {}'.format(token)
t = requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)

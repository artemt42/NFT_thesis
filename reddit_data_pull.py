# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 15:59:03 2022

@author: atishakov
"""


import requests
import os

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
    return res

api = api_request()
api.text

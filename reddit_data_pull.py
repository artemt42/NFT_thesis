# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 16:43:03 2022

@author: atishakov
"""

import reddit_api_key_request as new_key
import time
import json

def api_key_check():
    print(new_key.request())
    with open('api_keys/reddit_api.txt',"r") as f:
        api_reddit = f.read()
    api_reddit = json.loads(api_reddit)
    api_key = api_reddit["access_token"]
    return api_key

api_key_check()

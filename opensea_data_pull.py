# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 11:35:31 2022

@author: atishakov
"""

import requests
import json
import pandas as pd
import os
import pyodbc
import sqlalchemy as sa
import urllib
from io import StringIO
import ast
import time

opensea_key = os.getenv("opensea_api_2")
top_collections = pd.read_csv("top 100 collections.csv")
slugs = top_collections['slug'].values.tolist()
def dl_data(i,nxt='',to_df=[]):
    wait_time = 4
    headers = {"Accept": "application/json",
               "X-API-KEY":opensea_key}
    j = 0
    while nxt != None:
        collection_name = slugs[i]
        url = "https://api.opensea.io/api/v1/assets?collection_slug="+collection_name+"&order_direction=desc&limit=50&cursor="+nxt+"&include_orders=true"
        response = requests.request("GET", url, headers=headers)
        j+=50
        print(i,":",response.status_code, collection_name,nxt,str(j))
        if response.status_code != 200:
            return response,nxt,to_df
        parsed = json.loads(response.text)
        nxt = parsed['next']
        if not to_df:
            to_df = parsed["assets"]
        else:
            to_df.extend(parsed["assets"])
        # if j > 10200:
            # break
        time.sleep(wait_time)
    df_collections = pd.json_normalize(to_df)
    df_collections.to_csv("nft_collection_"+collection_name+".csv")

    return response,nxt,to_df

pages = len(slugs)
for i in range(15, pages):
    data = dl_data(i)
    s_code = data[0]
    while s_code.status_code != 200:
        data = dl_data(i,data[1],data[2])
        s_code = data[0]
# df_collections = pd.json_normalize(data[0])
# df_collections.to_csv("nft_collection_top14-.csv")
# response = data[1]
# ff = json.loads(response.text)
# df_collections = pd.json_normalize(ff,record_path="assets")
# df.to_csv("nfts_events.csv")

# fn='nfts_collections.csv'

# filepath = os.path.join("C:/Users/atish/Documents/GitHub/NFT_thesis",fn)

# df = pd.read_csv(filepath)

# stats = df['stats'].to_list()
# stats = [eval(i) for i in stats]
# df_stats = pd.DataFrame.from_dict(stats)

# df1 = df.drop('stats',axis=1).join(pd.DataFrame(df.stats.values.tolist()))

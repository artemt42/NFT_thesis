# -*- coding: utf-8 -*-
"""
Created on Sun May 29 13:36:15 2022

@author: atishakov
"""
import requests
import json
import pandas as pd
import os
import pyodbc
#import sqlalchemy as sa
import urllib
from io import StringIO
import ast
import time

os.chdir('G:/My Drive/BIM Thesis/Opensea data')
save_dir = 'C:/Users/atish/Documents/GitHub/NFT_thesis/events'

opensea_key = os.getenv("opensea_api_1")
top_collections = pd.read_csv("addresses and tokens.csv",low_memory=False)
top_collections = top_collections.sort_values(by='collection_slug')


slugs = top_collections['collection_slug'].values.tolist()
token_id = top_collections['token_id'].values.tolist()
log_file = open(save_dir+'/error_log.txt',"a")
def dl_data(i,nxt='',to_df=[]):
    wait_time = 1
    headers = {"Accept": "application/json",
               "X-API-KEY":opensea_key}
    j = 0
    while nxt != None:
        asset_id = top_collections['asset_contract_address'].values.tolist()
        asset = asset_id[i]
        token = token_id[i]
        collection_name = slugs[i]
        url = "https://api.opensea.io/api/v1/events?token_id="+token+"&asset_contract_address="+asset+"&cursor="+nxt+"&occurred_before=1640991600&occurred_after=1609455600"
        # asset = '0x39fa15e7dffd76bdeec83c9a1a8ef023661c9b6c'
        # token = '7'
        # nxt = ''
        # url = "https://api.opensea.io/api/v1/events?token_id="+token+"&asset_contract_address="+asset+"&cursor="+nxt #+"&occurred_before=1640991600&occurred_after=1609455600"

        response = requests.request("GET", url, headers=headers)
        j+=1
        # log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code) +"/n"
        # print(log_msg)
        # log_file.write(log_msg)
        if response.status_code != 200:
            log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code) +"/n"
            log_file.write(log_msg)
            print(log_msg)
            return response,nxt,to_df
        parsed = json.loads(response.text)
        nxt = parsed['next']
        if len(to_df)<1:
            to_df = parsed["asset_events"]
        else:
            to_df.extend(parsed["asset_events"])
        # if j > 10200:
            # break
        time.sleep(wait_time)

    return response,nxt,to_df

pages = len(slugs)
to_df = []
for i in range(180336, pages):
    if len(to_df) < 1:
        data = dl_data(i)
    else:
        data = dl_data(i,'',data[2])

    s_code = data[0]
    while s_code.status_code != 200: #retry functionality in case opensea returns response other than 200
        data = dl_data(i,data[1],data[2])
        s_code = data[0]

    this_name = slugs[i]
    prev_name = slugs[i-1]
    token = token_id[i]
    if i < 1:
        prev_name = this_name
    print(this_name,str(token))
    to_df = data[2]
    df_collections = pd.json_normalize(to_df)
    df_collections.to_csv(save_dir+"/nft_events_"+this_name+"-"+token+".csv",index=False)
    to_df = []
log_file.close()

        # to_df=[]
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

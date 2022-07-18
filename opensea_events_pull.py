# -*- coding: utf-8 -*-
"""
Created on Sun May 29 13:36:15 2022

@author: atishakov
"""
import requests
from requests.exceptions import SSLError
import itertools
import json
import pandas as pd
import os
import pyodbc
#import sqlalchemy as sa
import urllib
from io import StringIO
import ast
import time
from urllib3.exceptions import ProtocolError

curr_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events'
save_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\event_batches'
version = 2

opensea_key = os.getenv("opensea_api_"+str(version))
# df = pd.read_csv(os.path.join(curr_dir,"missing_events.csv"),low_memory=False)
df_collections = pd.read_csv(os.path.join(curr_dir,"nft_collection_source.csv"),low_memory=False)
df_collections = df_collections.sort_values(by='collection_slug')
# top_collections=df.groupby("collection_slug").sample(frac=0.7,random_state=6)

des_columns = ["asset_asset_contract_address","asset_id","asset_name","asset_token_id","auction_type","bid_amount","collection_slug","contract_address","created_date","dev_fee_payment_event_created_date","dev_fee_payment_event_event_timestamp","dev_fee_payment_event_event_type","dev_fee_payment_event_payment_token_address","dev_fee_payment_event_payment_token_decimals","dev_fee_payment_event_payment_token_eth_price","dev_fee_payment_event_payment_token_name","dev_fee_payment_event_payment_token_symbol","dev_fee_payment_event_payment_token_usd_price","dev_fee_payment_event_quantity","dev_fee_payment_event_transaction_block_hash","dev_fee_payment_event_transaction_block_number","dev_fee_payment_event_transaction_id","dev_fee_payment_event_transaction_timestamp","dev_fee_payment_event_transaction_transaction_hash","dev_fee_payment_event_transaction_transaction_index","dev_seller_fee_basis_points","duration","ending_price","event_timestamp","event_type","from_account_address","from_account_config","from_account_user_username","id","is_private","listing_time","payment_token_address","payment_token_decimals","payment_token_eth_price","payment_token_name","payment_token_symbol","payment_token_usd_price","quantity","seller_address","seller_config","seller_user_username","starting_price","to_account_address","to_account_config","to_account_user_username","total_price","transaction_block_hash","transaction_block_number","transaction_from_account_address","transaction_from_account_config","transaction_from_account_user_username","transaction_id","transaction_timestamp","transaction_to_account_address","transaction_to_account_config","transaction_to_account_user_username","transaction_transaction_hash","transaction_transaction_index","winner_account_address","winner_account_config","winner_account_user_username"]

# necessary_values = {'doodles-official':177,'boredapeyachtclub':539,'meebits':3047,'alienfrensnft':2000}

df_collections['address:token'] = df_collections['asset_contract_address']+":"+df_collections['token_id']

collection_tokens = {k: list(v) for k, v in df_collections.groupby('collection_slug')['address:token']}
del df_collections

# log_file = open(save_dir+'/error_log.txt',"a")
def dl_data(token,address,nxt='',to_df=[]):
    wait_time = 10 # if the response status code is not 200
    headers = {"Accept": "application/json",
               "X-API-KEY":opensea_key}
    j = 0
    response = ''

    while nxt != None:
        if token != '':
            url = "https://api.opensea.io/api/v1/events?event_type=successful&asset_contract_address="+address+"&token_id="+token+"&cursor="+nxt+"&occurred_before=1640991600&occurred_after=1622498400"
        else:
            url = "https://api.opensea.io/api/v1/events?event_type=successful&asset_contract_address="+address+"&cursor="+nxt+"&occurred_before=1640991600&occurred_after=1622498400"
            # print(url)
        response = requests.request("GET", url, headers=headers)
        log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code)
        print(log_msg)
    
        if response.status_code != 200:
            log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code)
            print(log_msg)
            time.sleep(wait_time)
            return response,nxt,to_df
    
        parsed = json.loads(response.text)
        nxt = parsed['next']
        if len(to_df)<1:
            to_df = parsed["asset_events"]
        else:
            to_df.extend(parsed["asset_events"])
        # if j > 10200:
            # break
        j+=1

    #--
    return response,nxt,to_df


collection_tokens_1 = dict(itertools.islice(collection_tokens.items(), 15,16))
collection_tokens_2 = dict(itertools.islice(collection_tokens.items(), 28,30))

del collection_tokens
to_df = []
header = False
if version == 1:
    token_dict = collection_tokens_1
elif version == 2:
    token_dict = collection_tokens_2

special = []
for key,value in token_dict.items():
    page_limit = len(token_dict[key])
    token_list = token_dict[key]
    collection_name = key
    k = 0
    for i in token_list:
        event_id = i.split(":")
        address = event_id[0]

        if 'sandbox' in key or 'decentraland' in key:
            token = ''
            if address in special:
                continue
            special.append(address)
        else:
            token = event_id[1]
        while True:
            try:
                # print(collection_name+" #"+str(i))
                if len(to_df) < 1:
                    data = dl_data(token,address)
                else:
                    data = dl_data(token,address,'',data[2])
                break
            except (ConnectionResetError,ProtocolError,ConnectionError,SSLError) as e:
                print("Connection error: "+str(e)+" \nRestarting in 10 seconds")
                time.sleep(10)
                continue
            if k % 5 == 0:
                time.sleep(2)
        s_code = data[0]
        while s_code.status_code != 200: #retry functionality in case opensea returns response other than 200
            data = dl_data(token,address,data[1],data[2])
            s_code = data[0]
    
        file_name = key

        to_df = data[2]
        df_collections = pd.json_normalize(to_df)
        df_columns = df_collections.columns

        col_names = dict()
        for cols in df_columns:
            cols1 = cols
            cols2 = cols
            if "." in cols:
                cols2 = cols.replace(".","_") #replaces dots with underscores, so that can be parsed in postgresql
            col_names[cols1]=cols2

        df_collections = df_collections.rename(col_names,axis=1) #renames columns as per dictionary

        df_collections = df_collections.drop(columns=[c for c in df_collections if c not in des_columns]) #drops columns if they are not in des_columns

        df_collections = df_collections.reindex(df_collections.columns.union(des_columns, sort=None), axis=1, fill_value='') #adds the columns again, but with empty values
        # df_collections["sampling"] = 'sampled'

        if len(to_df)>1:
            # print(df_collections.head())
            df_collections.to_csv(save_dir+r"\all_nft_sales"+str(version)+".csv",mode='a',
                                  index=False, columns=des_columns,header=header)
            header = False
            print(str(k)+": "+collection_name+" #"+str(i))
        else:
            print("empty collection token")
        print("")
        k+= 1
        to_df = []
# log_file.close()

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

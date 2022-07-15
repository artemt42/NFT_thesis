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
from urllib3.exceptions import ProtocolError

curr_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events'
save_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\event_batches'
version = 2

opensea_key = os.getenv("opensea_api_"+str(version))
df = pd.read_csv(os.path.join(curr_dir,"missing_events.csv"),low_memory=False)
# df = df.sort_values(by='collection_slug')
top_collections=df.groupby("collection_slug").sample(frac=0.4,random_state=7)
del df
des_columns = ["asset_asset_contract_address","asset_id","asset_name","asset_token_id","auction_type","bid_amount","collection_slug","contract_address","created_date","dev_fee_payment_event_created_date","dev_fee_payment_event_event_timestamp","dev_fee_payment_event_event_type","dev_fee_payment_event_payment_token_address","dev_fee_payment_event_payment_token_decimals","dev_fee_payment_event_payment_token_eth_price","dev_fee_payment_event_payment_token_name","dev_fee_payment_event_payment_token_symbol","dev_fee_payment_event_payment_token_usd_price","dev_fee_payment_event_quantity","dev_fee_payment_event_transaction_block_hash","dev_fee_payment_event_transaction_block_number","dev_fee_payment_event_transaction_id","dev_fee_payment_event_transaction_timestamp","dev_fee_payment_event_transaction_transaction_hash","dev_fee_payment_event_transaction_transaction_index","dev_seller_fee_basis_points","duration","ending_price","event_timestamp","event_type","from_account_address","from_account_config","from_account_user_username","id","is_private","listing_time","payment_token_address","payment_token_decimals","payment_token_eth_price","payment_token_name","payment_token_symbol","payment_token_usd_price","quantity","seller_address","seller_config","seller_user_username","starting_price","to_account_address","to_account_config","to_account_user_username","total_price","transaction_block_hash","transaction_block_number","transaction_from_account_address","transaction_from_account_config","transaction_from_account_user_username","transaction_id","transaction_timestamp","transaction_to_account_address","transaction_to_account_config","transaction_to_account_user_username","transaction_transaction_hash","transaction_transaction_index","winner_account_address","winner_account_config","winner_account_user_username","sampling"]

necessary_values = {'doodles-official':177,'boredapeyachtclub':539,'meebits':3047,'alienfrensnft':2000}

# top_collections['slug_address'] = top_collections['collection_slug']+","+top_collections['asset_contract_address']

top_collections_tokens = {k: list(v) for k, v in top_collections.groupby('collection_slug')['token_id']}
top_collections_address = {k: list(v) for k, v in top_collections.groupby('collection_slug')['asset_contract_address']}

# log_file = open(save_dir+'/error_log.txt',"a")
def dl_data(i,collection_name,address,nxt='',to_df=[]):
    wait_time = 4 # if the response status code is not 200
    headers = {"Accept": "application/json",
               "X-API-KEY":opensea_key}
    j = 0
    response = ''

    while nxt != None:
        token = str(i)
    
        url = "https://api.opensea.io/api/v1/events?asset_contract_address="+address+"&token_id="+token+"&collection_slug="+collection_name+"&cursor="+nxt+"&occurred_before=1640991600&occurred_after=1622498400"
        # print(url)
        response = requests.request("GET", url, headers=headers)
        log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code) +"/n"
        # print(log_msg)
    
        if response.status_code != 200:
            log_msg = collection_name+ ", token:"+token+", page:"+ str(j)+ ", status code:"+str(response.status_code) +"/n"
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

#
to_df = []
for key,value in necessary_values.items():
    address = top_collections_address[key][0]
    page_limit = len(top_collections_tokens[key])
    token_list = top_collections_tokens[key]
    collection_name = key
    header = True
    k = 0
    for i in token_list:
        while True:
            try:
                print(collection_name+" #"+str(i))
                if len(to_df) < 1:
                    data = dl_data(i,collection_name,address)
                else:
                    data = dl_data(i,collection_name,address,'',data[2])
                break
            except (ConnectionResetError,ProtocolError,ConnectionError) as e:
                print("Connection error: "+str(e)+" \nRestarting in 10 seconds")
                time.sleep(10)
                continue
        s_code = data[0]
        while s_code.status_code != 200: #retry functionality in case opensea returns response other than 200
            data = dl_data(i,collection_name,address,data[1],data[2])
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
        df_collections["sampling"] = 'sampled'

        if len(to_df)>10:
            # print(df_collections.head())
            df_collections.to_csv(save_dir+r"\nft_events_"+file_name+".csv",mode='a',
                                  index=False, columns=des_columns,header=header)
            print(str(k)+": "+collection_name+" #"+str(i))
            k+= 1
            if k > value:
                break
        else:
            print(key+":"+str(i)+" empty")
        header = False
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

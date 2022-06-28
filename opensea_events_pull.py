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

os.chdir('G:/My Drive/BIM Thesis/Opensea data')
save_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events'
version = 2

opensea_key = os.getenv("opensea_api_"+str(version))
df = pd.read_csv("addresses_and_tokens_new.csv",low_memory=False)
# df = df.sort_values(by='collection_slug')
top_collections=df.groupby("collection_slug").sample(frac=0.2,random_state=7)
del df
des_columns = ['approved_account','asset.asset_contract.address','asset.description','asset.id','asset.name','asset.token_id','asset.token_metadata','auction_type','bid_amount','collection_slug','contract_address','created_date','custom_event_name','dev_fee_payment_event','dev_fee_payment_event.asset','dev_fee_payment_event.asset_bundle','dev_fee_payment_event.auction_type','dev_fee_payment_event.created_date','dev_fee_payment_event.event_timestamp','dev_fee_payment_event.event_type','dev_fee_payment_event.payment_token.address','dev_fee_payment_event.payment_token.decimals','dev_fee_payment_event.payment_token.eth_price','dev_fee_payment_event.payment_token.image_url','dev_fee_payment_event.payment_token.name','dev_fee_payment_event.payment_token.symbol','dev_fee_payment_event.payment_token.usd_price','dev_fee_payment_event.quantity','dev_fee_payment_event.total_price','dev_fee_payment_event.transaction.block_hash','dev_fee_payment_event.transaction.block_number','dev_fee_payment_event.transaction.from_account','dev_fee_payment_event.transaction.id','dev_fee_payment_event.transaction.timestamp','dev_fee_payment_event.transaction.to_account','dev_fee_payment_event.transaction.transaction_hash','dev_fee_payment_event.transaction.transaction_index','dev_seller_fee_basis_points','duration','ending_price','event_timestamp','event_type','from_account.address','from_account.config','from_account.user.username','id','is_private','listing_time','payment_token.address','payment_token.decimals','payment_token.eth_price','payment_token.name','payment_token.symbol','payment_token.usd_price','quantity','seller.address','seller.config','seller.user.username','starting_price','to_account.address','to_account.config','to_account.user.username','total_price','transaction.block_hash','transaction.block_number','transaction.from_account.address','transaction.from_account.config','transaction.from_account.user','transaction.from_account.user.username','transaction.id','transaction.timestamp','transaction.to_account.address','transaction.to_account.config','transaction.to_account.user.username','transaction.transaction_hash','transaction.transaction_index','winner_account.address','winner_account.config','winner_account.user.username']

slugs = top_collections['collection_slug'].values.tolist()
token_id = top_collections['token_id'].values.tolist()
# log_file = open(save_dir+'/error_log.txt',"a")
def dl_data(i,nxt='',to_df=[]):
    wait_time = 3 # if the response status code is not 200
    headers = {"Accept": "application/json",
               "X-API-KEY":opensea_key}
    j = 0
    while nxt != None:
        asset_id = top_collections['asset_contract_address'].values.tolist()
        asset = asset_id[i]
        token = str(token_id[i])
        collection_name = slugs[i]
        url = "https://api.opensea.io/api/v1/events?token_id="+token+"&asset_contract_address="+asset+"&cursor="+nxt+"&occurred_before=1640991600&occurred_after=1622498400"
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
            # log_file.write(log_msg)
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


    return response,nxt,to_df

pages = len(slugs)
to_df = []
start = int(47424/4)
for i in range(15917, 16200):
    while True:
        try:
            if len(to_df) < 1:
                data = dl_data(i)
            else:
                data = dl_data(i,'',data[2])
            break
        except (ConnectionResetError,ProtocolError,ConnectionError) as e:
            print("Connection error: "+str(e)+" \nRestarting in 30 seconds")
            time.sleep(30)
            continue
    s_code = data[0]
    while s_code.status_code != 200: #retry functionality in case opensea returns response other than 200
        data = dl_data(i,data[1],data[2])
        s_code = data[0]

    this_name = slugs[i]
    prev_name = slugs[i-1]
    token = str(token_id[i])
    if i < 1:
        prev_name = this_name
    print(this_name,str(token))
    to_df = data[2]
    df_collections = pd.json_normalize(to_df)
    df_columns = df_collections.columns
    columns = [c for c in df_columns if c in des_columns]
    df_collections.to_csv(save_dir+"/nft_events_"+this_name+"-"+token+".csv",index=False, columns=columns)
    # with open("downloaded_events_"+str(version)+".txt","a") as dl_log:
    #     dl_log.write(this_name+"-"+token+"\n")
    # dl_log.close()
    # del dl_log
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

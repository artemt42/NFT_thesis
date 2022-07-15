# -*- coding: utf-8 -*-
"""
Created on Sun Jun 26 18:52:18 2022

@author: atishakov
"""

import zipfile
import os
from datetime import date
import time
import pandas as pd
import glob
from pandas.errors import EmptyDataError
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import numpy as np
import time
from itertools import chain

os.chdir(r'C:\Users\atish\Documents\GitHub\NFT_thesis\events')

path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events' # use your path
all_files = glob.glob(os.path.join(path , "*.csv"))
today = date.today()
# file_name = os.path.join(path,'nft_events_batch_'+str(today)+'.csv')
des_columns = ['approved_account','asset.asset_contract.address','asset.description','asset.id','asset.name','asset.token_id','asset.token_metadata','auction_type','bid_amount','collection_slug','contract_address','created_date','custom_event_name','dev_fee_payment_event','dev_fee_payment_event.asset','dev_fee_payment_event.asset_bundle','dev_fee_payment_event.auction_type','dev_fee_payment_event.created_date','dev_fee_payment_event.event_timestamp','dev_fee_payment_event.event_type','dev_fee_payment_event.payment_token.address','dev_fee_payment_event.payment_token.decimals','dev_fee_payment_event.payment_token.eth_price','dev_fee_payment_event.payment_token.image_url','dev_fee_payment_event.payment_token.name','dev_fee_payment_event.payment_token.symbol','dev_fee_payment_event.payment_token.usd_price','dev_fee_payment_event.quantity','dev_fee_payment_event.total_price','dev_fee_payment_event.transaction.block_hash','dev_fee_payment_event.transaction.block_number','dev_fee_payment_event.transaction.from_account','dev_fee_payment_event.transaction.id','dev_fee_payment_event.transaction.timestamp','dev_fee_payment_event.transaction.to_account','dev_fee_payment_event.transaction.transaction_hash','dev_fee_payment_event.transaction.transaction_index','dev_seller_fee_basis_points','duration','ending_price','event_timestamp','event_type','from_account.address','from_account.config','from_account.user.username','id','is_private','listing_time','payment_token.address','payment_token.decimals','payment_token.eth_price','payment_token.name','payment_token.symbol','payment_token.usd_price','quantity','seller.address','seller.config','seller.user.username','starting_price','to_account.address','to_account.config','to_account.user.username','total_price','transaction.block_hash','transaction.block_number','transaction.from_account.address','transaction.from_account.config','transaction.from_account.user','transaction.from_account.user.username','transaction.id','transaction.timestamp','transaction.to_account.address','transaction.to_account.config','transaction.to_account.user.username','transaction.transaction_hash','transaction.transaction_index','winner_account.address','winner_account.config','winner_account.user.username']
# columns.sort()
log = []
li = []
final_df = []
i = 0
empty_set = 0

columns = []
for filename in all_files:
    # print(filename)
    batch_file = pd.read_csv(filename,delimiter=",",nrows=10)
    for col in batch_file:
        # print(col)
        if col not in columns:
            columns.append(col)

for filename in all_files:
    batch_file = pd.read_csv(filename,delimiter=",",chunksize=25000)
    for t in batch_file:
        start = time.time()
        print(filename)
        for row in t.values:
            # print(row)
            conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
            engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')
            
            conn.autocommit = True
            cursor = conn.cursor()
            t['filename'] = filename
            col_names = dict()
            for cols in columns:
                cols1 = cols
                cols2 = cols
                if "." in cols:
                    cols2 = cols.replace(".","_")
                col_names[cols1]=cols2
            t = t.rename(col_names,axis=1)

            # df_upload = df_upload.drop(['metadata','maker','taker','fee_recipient','payment_token_contract'],1)
            
            
            cols = " varchar(8000), ".join(name for name in col_names)
            cols = cols +" varchar(8000)"
            # sql = '''CREATE TABLE DETAILS('''+cols+''');'''
            df_sql = t.to_sql(name="nft_events",con=engine,if_exists='append')


            # cursor.execute(sql)
            conn.commit()
            conn.close()
        end = time.time()
        print("--- %s seconds ---" % round((time.time() - start)/60))

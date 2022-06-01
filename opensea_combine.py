# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 16:54:58 2022

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
import glob
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import numpy as np
from itertools import chain

# directory = 'collections'

os.chdir("C:/Users/atish/Documents/GitHub/NFT_thesis")

# extension = 'csv'
# all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
# #combine all files in the list
# combined_csv = pd.concat([pd.read_csv(f,low_memory=False) for f in all_filenames])
# #export to csv
# combined_csv.to_csv( "top100_collections.csv", index=False, encoding='utf-8-sig')

# # for filename in os.listdir(directory):
# #     f = os.path.join(directory,filename)
# #     if os.path.isfile(f):
# #         csv_file = pd.read_csv(f,low_memory=False)

# =============================================================================
# flatten data within tables
# =============================================================================
temp_df = []
for chunk in pd.read_csv("collections/top100_collections.csv",chunksize=20000,low_memory=False):
    temp_df.append(chunk)
df = pd.concat(temp_df,axis=0)
del(temp_df)

df_sellOrders = df[['asset_contract.address','token_id','sell_orders']]
df_sellOrders = df_sellOrders[df_sellOrders.sell_orders.notnull()]
df_sellOrders_id = df_sellOrders[['asset_contract.address','token_id']]
df_sellOrders = df_sellOrders['sell_orders']
df_sellOrders = df_sellOrders.values.tolist()

dank = []
for item in df_sellOrders:
    if not isinstance(item, int):
        # print(eval(item))
        item = eval(item)
        dank.append(item)
    else:
        dank.append(item)

df_sellOrders_id = pd.DataFrame(df_sellOrders_id.values.tolist())
df_shitcunts = pd.DataFrame(list(chain.from_iterable(dank)))



df_shitcunts_metadata = df_shitcunts['metadata']
df_metadata = df_shitcunts_metadata.values.tolist()
df_ds = pd.DataFrame(df_metadata)
df_shitcunts_metadata = pd.DataFrame(df_ds['asset'].values.tolist())
df_shitcunts_metadata = df_shitcunts_metadata.join(df_ds['schema'])
meta_cols = [co + '_metadata' for co in df_shitcunts_metadata.columns]
m = dict()
for c in range(0,len(meta_cols)):
    m[df_shitcunts_metadata.columns[c]] = meta_cols[c]
df_shitcunts_metadata = df_shitcunts_metadata.rename(m,axis=1)

df_shitcunts = df_shitcunts.join(df_shitcunts_metadata)

# =============================================================================
# most braindead json normalizing below
# =============================================================================
df_shitcunts_maker = df_shitcunts['maker']
df_maker = df_shitcunts_maker.values.tolist()
df_shitcunts_maker = pd.DataFrame(df_maker)
m = dict()
meta_cols = [co + '_maker' for co in df_shitcunts_maker.columns]
for c in range(0,len(meta_cols)):
    m[df_shitcunts_maker.columns[c]] = meta_cols[c]
df_shitcunts_maker = df_shitcunts_maker.rename(m,axis=1)

df_shitcunts = df_shitcunts.join(df_shitcunts_maker)


df_shitcunts_taker = df_shitcunts['taker']
df_taker = df_shitcunts_taker.values.tolist()
df_shitcunts_taker = pd.DataFrame(df_maker)
m = dict()
meta_cols = [co + '_taker' for co in df_shitcunts_taker.columns]
for c in range(0,len(meta_cols)):
    m[df_shitcunts_taker.columns[c]] = meta_cols[c]
df_shitcunts_taker = df_shitcunts_taker.rename(m,axis=1)

df_shitcunts = df_shitcunts.join(df_shitcunts_taker)


df_shitcunts_fee = df_shitcunts['fee_recipient']
df_fee = df_shitcunts_fee.values.tolist()
df_shitcunts_fee = pd.DataFrame(df_fee)
m = dict()
meta_cols = [co + '_fee_recipient' for co in df_shitcunts_fee.columns]
for c in range(0,len(meta_cols)):
    m[df_shitcunts_fee.columns[c]] = meta_cols[c]
df_shitcunts_fee = df_shitcunts_fee.rename(m,axis=1)

df_shitcunts = df_shitcunts.join(df_shitcunts_fee)


df_shitcunts_contract = df_shitcunts['payment_token_contract']
df_fee = df_shitcunts_contract.values.tolist()
df_shitcunts_contract = pd.DataFrame(df_fee)
m = dict()
meta_cols = [co + '_payment_token_contract' for co in df_shitcunts_contract.columns]
for c in range(0,len(meta_cols)):
    m[df_shitcunts_contract.columns[c]] = meta_cols[c]
df_shitcunts_contract = df_shitcunts_contract.rename(m,axis=1)

df_shitcunts = df_shitcunts.join(df_shitcunts_contract)
# =============================================================================
#
# =============================================================================


df_sellOrders_new = df_sellOrders_id.join(df_shitcunts)

df_upload = df_sellOrders_new.rename(columns={0:'asset_contract.address',1:'token_id'})

conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

conn.autocommit = True
cursor = conn.cursor()

columns = df_upload.columns
col_names = dict()
for cols in columns:
    cols1 = cols
    cols2 = cols
    if "." in cols:
        cols2 = cols.replace(".","_")
    col_names[cols1]=cols2
df_upload = df_upload.rename(col_names,axis=1)
df_upload = df_upload.drop(['metadata','maker','taker','fee_recipient','payment_token_contract'],1)


cols = " varchar(8000), ".join(name for name in col_names)
cols = cols +" varchar(8000)"
# sql = '''CREATE TABLE DETAILS('''+cols+''');'''
df_sql = df_upload.to_sql(name="nft_collections_sell_orders",con=engine,if_exists='replace')


# cursor.execute(sql)
conn.commit()
conn.close()

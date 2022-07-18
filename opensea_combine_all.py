# -*- coding: utf-8 -*-
"""
Created on Sun Jul  3 18:57:29 2022

@author: atishakov
"""

import os
import pandas as pd
import glob
from datetime import date
from pandas.errors import EmptyDataError
from pathlib import Path
import re

import psycopg2
from sqlalchemy import create_engine

des_columns = ['approved_account','asset.asset_contract.address',
               # 'asset.description',
               'asset.id','asset.name','asset.token_id',
               # 'asset.token_metadata',
               'auction_type','bid_amount','collection_slug','contract_address','created_date','custom_event_name',
               'dev_fee_payment_event','dev_fee_payment_event.asset','dev_fee_payment_event.asset_bundle',
               'dev_fee_payment_event.auction_type','dev_fee_payment_event.created_date',
               'dev_fee_payment_event.event_timestamp','dev_fee_payment_event.event_type',
               'dev_fee_payment_event.payment_token.address','dev_fee_payment_event.payment_token.decimals',
               'dev_fee_payment_event.payment_token.eth_price',
               # 'dev_fee_payment_event.payment_token.image_url',
               'dev_fee_payment_event.payment_token.name','dev_fee_payment_event.payment_token.symbol',
               'dev_fee_payment_event.payment_token.usd_price','dev_fee_payment_event.quantity',
               'dev_fee_payment_event.total_price','dev_fee_payment_event.transaction.block_hash',
               'dev_fee_payment_event.transaction.block_number','dev_fee_payment_event.transaction.from_account',
               'dev_fee_payment_event.transaction.id','dev_fee_payment_event.transaction.timestamp',
               'dev_fee_payment_event.transaction.to_account','dev_fee_payment_event.transaction.transaction_hash',
               'dev_fee_payment_event.transaction.transaction_index','dev_seller_fee_basis_points','duration',
               'ending_price','event_timestamp','event_type','from_account.address','from_account.config',
               'from_account.user.username','id','is_private','listing_time','payment_token.address',
               'payment_token.decimals','payment_token.eth_price','payment_token.name',
               'payment_token.symbol','payment_token.usd_price','quantity','seller.address',
               'seller.config','seller.user.username','starting_price','to_account.address',
               'to_account.config','to_account.user.username','total_price','transaction.block_hash',
               'transaction.block_number','transaction.from_account.address','transaction.from_account.config',
               'transaction.from_account.user','transaction.from_account.user.username','transaction.id',
               'transaction.timestamp','transaction.to_account.address','transaction.to_account.config',
               'transaction.to_account.user.username','transaction.transaction_hash',
               'transaction.transaction_index','winner_account.address','winner_account.config','winner_account.user.username',
               'sampling']

des_columns1 = ['approved_account','asset_asset_contract_address',
               # 'asset_description',
               'asset_id','asset_name','asset_token_id',
               # 'asset_token_metadata',
               'auction_type','bid_amount','collection_slug','contract_address','created_date','custom_event_name',
               'dev_fee_payment_event','dev_fee_payment_event_asset','dev_fee_payment_event_asset_bundle',
               'dev_fee_payment_event_auction_type','dev_fee_payment_event_created_date',
               'dev_fee_payment_event_event_timestamp','dev_fee_payment_event_event_type',
               'dev_fee_payment_event_payment_token_address','dev_fee_payment_event_payment_token_decimals',
               'dev_fee_payment_event_payment_token_eth_price',
               # 'dev_fee_payment_event_payment_token_image_url',
               'dev_fee_payment_event_payment_token_name','dev_fee_payment_event_payment_token_symbol',
               'dev_fee_payment_event_payment_token_usd_price','dev_fee_payment_event_quantity',
               'dev_fee_payment_event_total_price','dev_fee_payment_event_transaction_block_hash',
               'dev_fee_payment_event_transaction_block_number','dev_fee_payment_event_transaction_from_account',
               'dev_fee_payment_event_transaction_id','dev_fee_payment_event_transaction_timestamp',
               'dev_fee_payment_event_transaction_to_account','dev_fee_payment_event_transaction_transaction_hash',
               'dev_fee_payment_event_transaction_transaction_index','dev_seller_fee_basis_points','duration',
               'ending_price','event_timestamp','event_type','from_account_address','from_account_config',
               'from_account_user_username','id','is_private','listing_time','payment_token_address',
               'payment_token_decimals','payment_token_eth_price','payment_token_name',
               'payment_token_symbol','payment_token_usd_price','quantity','seller_address',
               'seller_config','seller_user_username','starting_price','to_account_address',
               'to_account_config','to_account_user_username','total_price','transaction_block_hash',
               'transaction_block_number','transaction_from_account_address','transaction_from_account_config',
               'transaction_from_account_user','transaction_from_account_user_username','transaction_id',
               'transaction_timestamp','transaction_to_account_address','transaction_to_account_config',
               'transaction_to_account_user_username','transaction_transaction_hash',
               'transaction_transaction_index','winner_account_address','winner_account_config','winner_account_user_username',
               'sampling']

save_path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\event_batches' # use your path
path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\sampled' # use your path
path1 = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\non_sampled\full-year'
path2 = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\non_sampled\half-year'
all_paths = {'sampled':[path],'unsampled':[path1,path2]}
log_path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\error_logs'

today = date.today()

def logger(msg):
    # print(msg)
    log_file.write(msg+"\n")
    # print(msg+"\n")


def remove(csv):
    for f in csv:
        try:
            msg = "removing " + str(csv)
            print(msg)
            os.remove(csv)
            logger(msg)
        except Exception as e:
            # print(str(e))
            print("did not remove "+str(csv))
            logger(str(e))
                # break


## full year files

# for csv in all_files:
#     print(csv+"\n")
#     chunk_size = 15000
#     for chunk in pd.read_csv(csv,chunksize=chunk_size,low_memory=False,usecols=des_columns):
#         temp_df.append(chunk[(chunk['created_date'] > '2021-05-29') & (chunk['created_date'] < '2022-01-01')])
#         i+=chunk_size
#         print("lines imported:" +str(i))
#         # break
#     print('\n'*3)
# combined_df = pd.concat(temp_df,axis=0)
# del temp_df

# combined_df.to_csv(save_name)
# print("\nSaved as: "+csv+"\nnumber of lines imported: "+str(len(combined_df)))

## ----

## half year files
# j = 1
# for csv in all_files:
#     temp_df= []
#     print(csv)
#     chunk_size = 50000
#     i = 0
#     for chunk in pd.read_csv(csv,chunksize=chunk_size,low_memory=False,usecols=des_columns):
#         temp_df.append(chunk[(chunk['created_date'] > '2021-05-29') & (chunk['created_date'] < '2022-01-01')])

#         # print("line: "+str(len(chunk)))
#         # print("file: "+str(j)+"/"+str(len(all_files))+", line: "+str("{:,}".format(i)))
#         if i < 1:
#             print("there are 78 columns in this dataset: "+str(len(chunk.columns)==78))
#         i+=chunk_size
#         # break
#     print("file: "+str(j)+"/"+str(len(all_files))+", line: "+str("{:,}".format(i)))
#     print('\n'*1)
#     combined_df = pd.concat(temp_df,axis=0)

#     save_name = Path(csv).stem
#     save_loc = os.path.join(save_path,save_name)
#     combined_df.to_csv(save_loc+".csv",columns=des_columns,index=False)

#     print("\nSaved as: "+csv+"\nnumber of lines cleaned: "+str(len(combined_df)))
#     j +=1
#     del temp_df
#     del combined_df
#     # remover(csv)

## ----

## combine all files into one huge csv

# header = True
# i = 0
# j = 1
# k = 1
# dicts = ['sampled','unsampled']
# absolute_total = 0

# for dict_key in dicts:
#     for folder in all_paths[dict_key]:
#         all_files = glob.glob(os.path.join(folder , "*.csv"))
#         # print(folder)
#         total_files = str(len(all_files))
#         absolute_total += (len(all_files))
#         for csv in all_files:
#             log_file = open(log_path+'\log_event_combiner.txt',"a")
#             logger(csv)
#             # print(csv)
#             # chunk_size = 75000
#             try:
#                 chunk = pd.read_csv(csv,low_memory=False,sep=",")
#                 chunk = chunk[(chunk['created_date'] > '2021-05-29') & (chunk['created_date'] < '2022-01-01')]
#                 chunk['sampling'] = dict_key
#                 msg = "Loading "+csv
#                 logger(msg)
#             except EmptyDataError as e:
#                 slug = Path(csv).stem.replace('nft_events_','')
#                 token_id = re.search('(\d+)[^-]*$',slug).group(1)
#                 slug = slug.replace("-"+token_id,'')
#                 sub_df = {'approved_account':'','asset.asset_contract.address':'','asset.id':'',
#                                 'asset.name':'','asset.token_id':token_id,'auction_type':'','bid_amount':'',
#                                 'collection_slug':slug,'contract_address':'','created_date':'','custom_event_name':'',
#                                 'dev_fee_payment_event':'','dev_fee_payment_event.asset':'','dev_fee_payment_event.asset_bundle':'',
#                                 'dev_fee_payment_event.auction_type':'','dev_fee_payment_event.created_date':'',
#                                 'dev_fee_payment_event.event_timestamp':'','dev_fee_payment_event.event_type':'',
#                                 'dev_fee_payment_event.payment_token.address':'','dev_fee_payment_event.payment_token.decimals':'',
#                                 'dev_fee_payment_event.payment_token.eth_price':'',
#                                 # 'dev_fee_payment_event.payment_token.image_url',
#                                 'dev_fee_payment_event.payment_token.name':'','dev_fee_payment_event.payment_token.symbol':'',
#                                 'dev_fee_payment_event.payment_token.usd_price':'','dev_fee_payment_event.quantity':'',
#                                 'dev_fee_payment_event.total_price':'','dev_fee_payment_event.transaction.block_hash':'',
#                                 'dev_fee_payment_event.transaction.block_number':'','dev_fee_payment_event.transaction.from_account':'',
#                                 'dev_fee_payment_event.transaction.id':'','dev_fee_payment_event.transaction.timestamp':'',
#                                 'dev_fee_payment_event.transaction.to_account':'','dev_fee_payment_event.transaction.transaction_hash':'',
#                                 'dev_fee_payment_event.transaction.transaction_index':'','dev_seller_fee_basis_points':'','duration':'',
#                                 'ending_price':'','event_timestamp':'','event_type':'','from_account.address':'','from_account.config':'',
#                                 'from_account.user.username':'','id':'','is_private':'','listing_time':'','payment_token.address':'',
#                                 'payment_token.decimals':'','payment_token.eth_price':'','payment_token.name':'',
#                                 'payment_token.symbol':'','payment_token.usd_price':'','quantity':'','seller.address':'',
#                                 'seller.config':'','seller.user.username':'','starting_price':'','to_account.address':'',
#                                 'to_account.config':'','to_account.user.username':'','total_price':'','transaction.block_hash':'',
#                                 'transaction.block_number':'','transaction.from_account.address':'','transaction.from_account.config':'',
#                                 'transaction.from_account.user':'','transaction.from_account.user.username':'','transaction.id':'',
#                                 'transaction.timestamp':'','transaction.to_account.address':'','transaction.to_account.config':'',
#                                 'transaction.to_account.user.username':'','transaction.transaction_hash':'',
#                                 'transaction.transaction_index':'','winner_account.address':'','winner_account.config':'',
#                                 'winner_account.user.username':'','sampling':dict_key}
#                 chunk = pd.DataFrame([sub_df])
#                 msg = ": Loading slug and token_id from file name"
#                 logger(str(e)+msg)


#             chunk = chunk.reindex(chunk.columns.union(des_columns, sort=False), axis=1, fill_value='')
#             # chunk['sampling'] = dict_key

#             chunk.to_csv(os.path.join(save_path, "nft_events_all.csv"),
#                           header=header, columns=des_columns, mode='a',
#                           index=False,sep=",",quotechar='"',encoding='utf-8')
#             header = False

#             i+=len(chunk)
#             print("file: "+str(j)+"/"+str(len(all_files))+","+" absolute total "+str(k)+"/"+str(absolute_total)+" line: "+str("{:,}".format(i)))
#             logger("Added "+csv+ " to file")
#             logger("file: "+str(j)+"/"+str(len(all_files))+","+" absolute total "+str(k)+"/"+str(absolute_total)+", line: "+str("{:,}".format(i))+"\n")
#             j+=1
#             k+=1
#             log_file.close()
#         j = 1


# print("\nDone!")
# print("Successfully combined "+str("{:,}".format(i))+" lines from "+str(absolute_total)+" files.")
# remove(all_files)

## many little semi-colon separated values

# # header = True
# i = 0
# j = 1
# total = 72870300
# temp_df= []
# chunk_size = 500000
# big_csv = os.path.join(path, "nft_events_batch_all.csv")

# for chunk in pd.read_csv(big_csv,chunksize=chunk_size,low_memory=False,usecols=des_columns):
#     chunk.to_csv(os.path.join(save_path, "nft_events_batch_"+str(j)+".csv"),
#                   columns=des_columns, index=False, sep=";")
#     # header = False

#     # temp_df.append(chunk[(chunk['created_date'] > '2021-05-29') & (chunk['created_date'] < '2022-01-01')])
#     i+=len(chunk)
#     print("file: "+str(j)+", line: "+str("{:,}".format(i))+" / "+str("{:,}".format(total)))
#     j+=1


# print("\nDone!")
# print("Successfully combined "+str("{:,}".format(i))+" lines from "+big_csv)
# # remove(all_files)


chunk_size = 100000
csv1 = os.path.join(save_path, "all_nft_sales1.csv")
csv2 = os.path.join(save_path, "all_nft_sales2.csv")
total = 77271127
i = 0

csv_sales_1 = pd.read_csv(csv1,low_memory=False)
csv_sales_2 = pd.read_csv(csv2,low_memory=False)

csv_sales = pd.concat([csv_sales_1,csv_sales_2])

# for chunk in pd.read_csv(big_csv,chunksize=chunk_size,low_memory=False,usecols=des_columns):

conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

conn.autocommit = True
cursor = conn.cursor()

# columns = des_columns
# col_names = dict()
# for cols in columns:
#     cols1 = cols
#     cols2 = cols
#     if "." in cols:
#         cols2 = cols.replace(".","_")
#     col_names[cols1]=cols2

# df_upload = chunk.rename(col_names,axis=1)
df_upload = csv_sales.copy()
for cols in df_upload:
    df_upload[cols] = df_upload[cols].astype(str).copy()
# df_upload = df_upload.drop(['metadata','maker','taker','fee_recipient','payment_token_contract'],1)
df_upload = df_upload.reindex(df_upload.columns.union(des_columns1, sort=False), axis=1, fill_value='')

cols = " varchar(1000), ".join(name for name in des_columns1)
cols = cols +" varchar(1000)"
# sql = '''CREATE TABLE DETAILS('''+cols+''');'''
df_sql = df_upload.to_sql(name="nft_sale_events",con=engine,if_exists='append',index=False)


# cursor.execute(sql)
conn.commit()
conn.close()

# chunk_len = len(chunk)
# i += chunk_len

# print("upserted "+str("{:,}".format(i)) +" out of "+str("{:,}".format(total)))


print("\nDone!")
# print("Successfully upserted "+str("{:,}".format(i))+" lines from "+big_csv)

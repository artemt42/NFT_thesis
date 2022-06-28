# -*- coding: utf-8 -*-
"""
Created on Sat Jun  4 13:18:17 2022

@author: atishakov
"""

import pandas as pd
import glob
import os
from pandas.errors import EmptyDataError
from datetime import date

def create_batch(path):
    # path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events' # use your path
    all_files = glob.glob(os.path.join(path , "nft_events_*.csv"))
    today = date.today()
    file_name = os.path.join(path,'nft_events_batch_'+str(today)+'.csv')
    des_columns = ['approved_account','asset.asset_contract.address','asset.description','asset.id','asset.name','asset.token_id','asset.token_metadata','auction_type','bid_amount','collection_slug','contract_address','created_date','custom_event_name','dev_fee_payment_event','dev_fee_payment_event.asset','dev_fee_payment_event.asset_bundle','dev_fee_payment_event.auction_type','dev_fee_payment_event.created_date','dev_fee_payment_event.event_timestamp','dev_fee_payment_event.event_type','dev_fee_payment_event.payment_token.address','dev_fee_payment_event.payment_token.decimals','dev_fee_payment_event.payment_token.eth_price','dev_fee_payment_event.payment_token.image_url','dev_fee_payment_event.payment_token.name','dev_fee_payment_event.payment_token.symbol','dev_fee_payment_event.payment_token.usd_price','dev_fee_payment_event.quantity','dev_fee_payment_event.total_price','dev_fee_payment_event.transaction.block_hash','dev_fee_payment_event.transaction.block_number','dev_fee_payment_event.transaction.from_account','dev_fee_payment_event.transaction.id','dev_fee_payment_event.transaction.timestamp','dev_fee_payment_event.transaction.to_account','dev_fee_payment_event.transaction.transaction_hash','dev_fee_payment_event.transaction.transaction_index','dev_seller_fee_basis_points','duration','ending_price','event_timestamp','event_type','from_account.address','from_account.config','from_account.user.username','id','is_private','listing_time','payment_token.address','payment_token.decimals','payment_token.eth_price','payment_token.name','payment_token.symbol','payment_token.usd_price','quantity','seller.address','seller.config','seller.user.username','starting_price','to_account.address','to_account.config','to_account.user.username','total_price','transaction.block_hash','transaction.block_number','transaction.from_account.address','transaction.from_account.config','transaction.from_account.user','transaction.from_account.user.username','transaction.id','transaction.timestamp','transaction.to_account.address','transaction.to_account.config','transaction.to_account.user.username','transaction.transaction_hash','transaction.transaction_index','winner_account.address','winner_account.config','winner_account.user.username']
    # columns.sort()
    log = []
    li = []
    final_df = []
    i = 0
    empty_set = 0
    for filename in all_files:
        print(filename)
        columns = []
        try:
            df = pd.read_csv(filename, index_col=None, header=0,low_memory=False)
            li.append(df)
        except EmptyDataError:
            empty_set += 1
            continue
        frame = pd.concat(li, axis=0, ignore_index=True)
        # to_df_check = i%100 == 0
        msg = "Combining "+filename
        # print(msg)
        log.append(msg)
        if i < 1:
            final_df = frame
        else:
            final_df = pd.concat([final_df,frame], axis=0, ignore_index=True)
        i+=1
        li = []
    df_columns = final_df.columns
    columns = [c for c in df_columns if c in des_columns]
    final_df.to_csv(file_name,columns=columns,index=False)
    # del final_df
    msg = "Combined "+str(i)+"/"+str(len(all_files)-empty_set)
    log.append(msg)
    return log,all_files,file_name

path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events' # use your path

create_batch(path)
# final_df.to_csv(path+str(i)+"_batch.csv",columns=columns,index=False)
# final_df.drop(columns,axis=1)

# df = pd.read_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\events\nft_events_alienfrens.csv",low_memory=False)
# df.to_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\events\nft_events_alienfrens2.csv",columns=des_columns,index=False)

# all_cols = final_df.columns

# for f in all_cols:
#     with open(r"C:\Users\atish\Documents\GitHub\NFT_thesis\events\ggg.txt","a") as file:
#         file.write(f)
#         file.write("\n")
#     file.close()

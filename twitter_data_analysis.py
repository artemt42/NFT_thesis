# -*- coding: utf-8 -*-
"""
Created on Sat May 28 14:06:30 2022

@author: atishakov
"""

import pandas as pd
import os
import json
import glob
import csv
from os import listdir
from os.path import isfile, join
import numpy as np

# =============================================================================
# Combine csv files into one
# =============================================================================
# path = 'C:/Users/atish/Documents/GitHub/NFT_thesis/tweets/'
# os.chdir(path)
# onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
# final_list = []

# for name in onlyfiles:
#     # name = "adidas Originals Into the Metaverse nft.csv"
#     df = pd.read_csv(name, index_col=None, header=0)
#     df['search_term'] = name[:-4]
#     final_list.append(df)
#     print(name)

# final_df = pd.concat(final_list,axis=0,ignore_index=True)
# final_df.to_csv("all tweets.csv")

    # df_list = tolis

# =============================================================================
# 
# =============================================================================

path = 'C:/Users/atish/Google Drive/BIM Thesis/Twitter Data/'
os.chdir(path)

df_original = pd.read_csv("all tweets.csv")

df = df_original.drop(['Unnamed: 0.1', 'Unnamed: 0'],axis=1)

df['likes'] = df['likes'].apply(pd.to_numeric)
df['retweets'] = df['retweets'].apply(pd.to_numeric)

df = df.drop_duplicates(subset='TweetId',keep='last')
df = df.to_csv("all tweets adj.csv")

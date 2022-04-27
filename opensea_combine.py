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

df = pd.read_csv("collections/top100_collections.csv",low_memory=False)
# df = pd.read_csv("collections/top100_collections.csv",low_memory=False,nrows=100)
df = df.drop('Unnamed: 0',1)

# conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

conn.autocommit = True
cursor = conn.cursor()

columns = df.columns
col_names = dict()
for cols in columns:
    cols1 = cols
    cols2 = cols
    if "." in cols:
        cols2 = cols.replace(".","_")
    col_names[cols1]=cols2
df = df.rename(col_names,axis=1)

cols = " varchar(3000), ".join(name for name in col_names)
cols = cols +" varchar(3000)"
# sql = '''CREATE TABLE DETAILS('''+cols+''');'''
df_sql = df.to_sql(name="nft_collections",con=engine)


cursor.execute(sql)
conn.commit()
conn.close()

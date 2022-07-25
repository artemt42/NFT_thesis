import pandas as pd
import psycopg2
import time
from sqlalchemy import create_engine
import os
import json
import re
import glob

def pull_events(cols,table):
    conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
    # engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')
    
    desired_columns = cols
    event_type_1 = 'successful'
    
    #slug = 'cool-cats-nft'
    
    query_col_names = "select "+','.join(desired_columns)+" \n"
    query_table = "from public."+table+" \n"
    # query_clauses = "where event_type = '"+event_type_1+"'"#+" and collection_slug = '"+slug+"'"
    
    query = query_col_names + query_table #+ query_clauses
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute(query)
    
    df = pd.DataFrame(cursor.fetchall(),columns=desired_columns)
    
    conn.close()

    return df

def pull_tweets(cols,table):
    conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
    # engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

    desired_columns = cols

    #slug = 'cool-cats-nft'
    
    query_col_names = "select *"+" \n"
    query_table = "from public."+table+" \n"

    query = query_col_names + query_table #+ query_clauses
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute(query)
    
    df = pd.DataFrame(cursor.fetchall(),columns=desired_columns)
    
    conn.close()

    return df

# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 11:35:31 2022

@author: atishakov
"""

import requests
import json
import pandas as pd
import os

to_df = []
for i in range(0,1001):
    try:
        url = "https://api.opensea.io/api/v1/collections?offset="+str(i)+"&limit=300"
        
        headers = {"Accept": "application/json"}
    
        response = requests.request("GET", url, headers=headers)
        print(i,":",response.status_code)
        parsed = json.loads(response.text)
    
        for p in parsed.values():
            for pp in p:
                to_df.append(pp)
    except Exception as e:
        i = 1001
        print(e)

df = pd.DataFrame.from_dict(to_df)
df.to_csv("nfts_sample.csv")

import pyodbc
import pandas as pd
# insert data from csv file into dataframe.
# working directory for csv file: type "pwd" in Azure Data Studio or Linux
# working directory in Windows c:\users\username
df = pd.read_csv("nfts_sample.csv")
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'ARTEMS-LAPTOP\SQLEXPRESS'
database = 'nfts'
username = 'ARTEMS-LAPTOP\atishakov'
password = os.getenv('pcpwd')
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# Insert 4Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO nfts.opensea_sample_data "+df.columns+" values(?,?,?)", row.DepartmentID, row.Name, row.GroupName)
cnxn.commit()
cursor.close()

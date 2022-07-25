# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 19:23:08 2022

@author: atishakov
"""

# importing libraries and packages
import snscrape.modules.twitter as sntwitter
import pandas as pd
import json
import os
import time
from datetime import datetime,timedelta
import psycopg2
from sqlalchemy import create_engine

save_dir = r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users'

def js(d):
    return json.loads(d)

def data_extract(filename):
    with open(filename,"r") as f:
        data = js(f.read())
    # tw_date = js(tw_date)
    return data


# search_data = pd.read_csv('search_terms.csv')
# search_data['start_date'] = pd.to_datetime(search_data['start_date'])
# coll_name = search_data['search_terms']
# # coll_name = {'Doodles':'Doodles nft',}
# # log_file = open('tweets/error_log.txt',"a")
# header = True
# j = 0
# for search_term in coll_name[23:]:
#     temp = search_data.transpose().to_dict()
#     # Creating list to append tweet data
#     max_date = datetime.strptime('2022-01-01','%Y-%m-%d')
#     earliest_date = temp[j]['start_date']
#     if not pd.isna(earliest_date) and earliest_date > datetime.strptime('2021-06-01','%Y-%m-%d'):
#         start_date = earliest_date - timedelta(days=7)
#     else:
#         start_date = datetime.strptime('2021-06-01','%Y-%m-%d')

#     end_date = start_date + timedelta(days=1)

#     search_term_df = search_term
#     search_term_list = []
#     if ' ' in search_term:
#         search_term_list.append(search_term)
#         search_term_list.append(search_term.replace(' ','-'))
#         search_term_list.append(search_term.replace(' ','_'))
#         search_term_list.append(search_term.replace(' ',''))
#         search_term = '('+' OR '.join(search_term_list)+')'
#     print(search_term)
#     j += 1
#     k = 0
#     while start_date < max_date:
#         tweets_list2 = []
#         since = datetime.strftime(start_date,'%Y-%m-%d')
#         until = datetime.strftime(end_date,'%Y-%m-%d')
#         # nft_words = ['nft','non-fungible token','non-fungible-token','nonfungibletoken','non fungible token'] #ensure that only nft tweets are found
#         # if not any(word in str.lower(search_term) for word in nft_words):
#         #     search_term += " nft"
#         to_search = search_term +' until:'+until+' since:'+since+' min_faves:1'
#         try:
#             for i,tweet in enumerate(sntwitter.TwitterSearchScraper(to_search).get_items()):
#                 if i>2500:
#                     break
#                 tweets_list2.append([tweet.date, tweet.id, tweet.rawContent,
#                                      tweet.user.username, tweet.hashtags,
#                                      tweet.inReplyToTweetId,tweet.likeCount,
#                                      tweet.retweetCount])
#             to_log = 'downloaded nft collection: '+search_term+' until: '+until+' count of tweets: ',str(len(tweets_list2))
#             print(to_log)
#             # log_file.write(to_log)
#         except TypeError as e:
#             to_log = "no data for "+search_term+' until '+until+' '+str(e)
#             print(to_log)
#             # log_file.write(to_log)
#         if k % 3 == 0:
#             time.sleep(3)
#         start_date = end_date
#         end_date = end_date + timedelta(days=1)
#         # Creating a dataframe from the tweets list above
#         tweets_df2 = pd.DataFrame(tweets_list2, columns=['Datetime', 'TweetId',
#                                                          'Text', 'Username',
#                                                          'Hashtags',
#                                                          'reply_TweetId',
#                                                          'likes','retweets'])
#         tweets_df2['search_term'] = search_term_df

#         tweets_df2.to_csv(save_dir+r"\more_tweets.csv",mode='a',
#                               index=False,header=header)
#         header = False
#         k += 1

# log_file.close()

def get_user_data(i, search_term):
    # log_file = open('twitter_users/error_log.txt',"a")
    print(i,end=":")
    # while True:
    try:
        user = sntwitter.TwitterUserScraper(search_term).entity
        user_data = [user.id,
                        user.username,
                        user.verified,
                        user.followersCount,
                        user.friendsCount,
                        user.statusesCount,
                        user.favouritesCount,
                        user.rawDescription,
                        user.renderedDescription,
                        user.descriptionLinks,
                        user.created,
                        user.location,
                        user.protected,
                        user.link,
                        user.label]
        to_log = 'downloaded user data for: '+search_term
        print(to_log)
    except (AttributeError,KeyError) as e:
        to_log = "no data for "+search_term
        user_data = [0,search_term,False,0,0,0,0,"no data","no data",[],'1970-01-01 00:00:00+00:00','',False,None,None]
        print(to_log)
        print(e)

    # log_file.write(to_log)
    # log_file.close()
    i += 1
    return user_data, i

# tweets_df = get_tweets()
# filtered_df = tweets_df[(tweets_df['retweets']>5) & (tweets_df['likes']>5)]

user_name = pd.read_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\missinguserdata_final_final.csv')
user_name = user_name['username'].values.tolist()
# count = user_name['tweetid'].values.tolist()
# user_name.sort()
# Creating list to append tweet data
user_data = []
i = 0
for search_term in user_name:
    function_data = get_user_data(i, search_term)
    user = function_data[0]
    user_data.append(user)


    i = function_data[1]
    time.sleep(1)

    # Creating a dataframe from the tweets list above
    users_df = pd.DataFrame(user_data, columns=["id",
                                                "username",
                                                "verified",
                                                "followers_count",
                                                "following_count",
                                                "statuses_count",
                                                "favourites_count",
                                                "raw_description",
                                                "rendered_description",
                                                "description_links",
                                                "user_creation_date",
                                                "location",
                                                "protected",
                                                "links",
                                                "user_label"])
    users_df.to_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\more_user_data.csv',
                    mode='a',index=False,header=False)
    user_data = []

# pee = pd.read_csv('twitter_users/all_user_data_first_half.csv')
# e = pd.read_csv('twitter_users/all_user_data.csv')
# f = pee.append(e)

# f.to_csv('twitter_users/all_user_data_combined.csv')

# user_df = pd.read_csv("twitter_users/all tweets adj.csv",encoding=('utf-8'))
# user_list_df = user_df['Username'].unique()
# user_list_df.sort()
# user_list_df = pd.DataFrame(user_list_df,columns = ['Username'])
# user_list_df.to_csv("twitter_users.csv")


# tweet_cols = ['TweetId','Datetime', 'Text', 'search_term',
#                 'Username','reply_TweetId',
#                 'likes', 'retweets','Hashtags']

# save_path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\event_batches' # use your path
# path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\sampled' # use your path
# path1 = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\non_sampled\full-year'
# path2 = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\batch_files\non_sampled\half-year'
# all_paths = {'sampled':[path],'unsampled':[path1,path2]}
# log_path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events\error_logs'

# # chunk_size = 100000
# csv1 = os.path.join(save_path, "all_nft_sales1.csv")
# csv2 = os.path.join(save_path, "all_nft_sales2.csv")
# # total = 77271127
# # i = 0

# tweets_1 = pd.read_csv(csv1,low_memory=False)
# tweets_2 = pd.read_csv(csv2,low_memory=False)

# tweets = pd.concat([tweets_1,tweets_2])

# # for chunk in pd.read_csv(big_csv,chunksize=chunk_size,low_memory=False,usecols=des_columns):

# conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
# engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

# conn.autocommit = True
# cursor = conn.cursor()

# # columns = des_columns
# # col_names = dict()
# # for cols in columns:
# #     cols1 = cols
# #     cols2 = cols
# #     if "." in cols:
# #         cols2 = cols.replace(".","_")
# #     col_names[cols1]=cols2

# # df_upload = chunk.rename(col_names,axis=1)
# df_upload = tweets.copy()
# for cols in df_upload:
#     df_upload[cols] = df_upload[cols].astype(str).copy()
# df_upload = df_upload.reindex(df_upload.columns.union(tweet_cols, sort=False), axis=1, fill_value='')

# cols = " varchar(1000), ".join(name for name in tweet_cols)
# cols = cols +" varchar(1000)"
# # sql = '''CREATE TABLE DETAILS('''+cols+''');'''
# df_sql = df_upload.to_sql(name="tweets",con=engine,if_exists='append',index=False)


# # cursor.execute(sql)
# conn.commit()
# conn.close()

# chunk_len = len(chunk)
# i += chunk_len

# print("upserted "+str("{:,}".format(i)) +" out of "+str("{:,}".format(total)))


# print("\nDone!")
# print("Successfully upserted "+str("{:,}".format(i))+" lines from "+big_csv)


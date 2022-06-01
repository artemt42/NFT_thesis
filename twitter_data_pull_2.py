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

os.chdir('C:/Users/atish/Documents/GitHub/NFT_thesis')

def js(d):
    return json.loads(d)

def data_extract(filename):
    with open(filename,"r") as f:
        data = js(f.read())
    # tw_date = js(tw_date)
    return data

def get_tweets():
    coll_name = data_extract('collection_names_2.txt')['collection_names'][18:]
    # coll_name = {'Doodles':'Doodles nft',}
    log_file = open('tweets/error_log.txt',"a")

    for search_term in coll_name:
        # Creating list to append tweet data
        max_date = datetime.strptime('2022-01-01','%Y-%m-%d')
        start_date = datetime.strptime('2021-01-01','%Y-%m-%d')
        end_date = start_date + timedelta(days=1)
        tweets_list2 = []
        while start_date < max_date:
            since = datetime.strftime(start_date,'%Y-%m-%d')
            until = datetime.strftime(end_date,'%Y-%m-%d')
            nft_words = ['nft','non-fungible token','non-fungible-token','nonfungibletoken','non fungible token'] #ensure that only nft tweets are found
            if not any(word in str.lower(search_term) for word in nft_words):
                search_term += " nft"
            to_search = search_term +' until:'+until+' since:'+since+' min_faves:1'
            try:
                for i,tweet in enumerate(sntwitter.TwitterSearchScraper(to_search).get_items()):
                    if i>1000:
                        break
                    tweets_list2.append([tweet.date, tweet.id, tweet.content,
                                         tweet.user.username, tweet.hashtags,
                                         tweet.inReplyToTweetId,tweet.likeCount,
                                         tweet.retweetCount])
                to_log = 'downloaded nft collection: '+search_term+' until: '+until+' count of tweets: ',str(len(tweets_list2))
                print(to_log)
                log_file.write(to_log)
            except TypeError as e:
                to_log = "no data for "+search_term+' until '+until+' '+str(e)
                log_file.write(to_log)
            time.sleep(2)
            start_date = end_date
            end_date = end_date + timedelta(days=1)
        # Creating a dataframe from the tweets list above
        tweets_df2 = pd.DataFrame(tweets_list2, columns=['Datetime', 'TweetId',
                                                         'Text', 'Username',
                                                         'Hashtags',
                                                         'reply_TweetId',
                                                         'likes','retweets'])
        tweets_df2.to_csv('tweets/'+search_term+'.csv')

    log_file.close()

tweets_df = get_tweets()
# filtered_df = tweets_df[(tweets_df['retweets']>5) & (tweets_df['likes']>5)]

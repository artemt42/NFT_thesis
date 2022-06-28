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

def get_user_data(i, search_term):
    log_file = open('twitter_users/error_log.txt',"a")
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

    log_file.write(to_log)
    log_file.close()
    i += 1
    return user_data, i

# tweets_df = get_tweets()
# filtered_df = tweets_df[(tweets_df['retweets']>5) & (tweets_df['likes']>5)]

user_name = pd.read_csv('twitter_users/twitter_users.csv')['Username']
user_name = user_name.values.tolist()
user_name.sort()
# Creating list to append tweet data
user_data = []
i = 0
for search_term in user_name[36993:]:
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
users_df.to_csv('twitter_users/all_user_data.csv')

# pee =  pd.read_csv('twitter_users/twitter_users.csv')['Username']
# user_df = pd.read_csv("twitter_users/all tweets adj.csv",encoding=('utf-8'))
# user_list_df = user_df['Username'].unique()
# user_list_df.sort()
# user_list_df = pd.DataFrame(user_list_df,columns = ['Username'])
# user_list_df.to_csv("twitter_users.csv")

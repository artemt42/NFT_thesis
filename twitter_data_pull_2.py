# -*- coding: utf-8 -*-
"""
Created on Sat Mar 26 19:23:08 2022

@author: atishakov
"""

# importing libraries and packages
import snscrape.modules.twitter as sntwitter
import pandas as pd

# # Creating list to append tweet data
# tweets_list1 = []

# # Using TwitterSearchScraper to scrape data and append tweets to list
# for i,tweet in enumerate(sntwitter.TwitterSearchScraper('from:jack').get_items()): #declare a username 
#     if i>1000: #number of tweets you want to scrape
#         break
#     tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username]) #declare the attributes to be returned
    
# # Creating a dataframe from the tweets list above 
# tweets_df1 = pd.DataFrame(tweets_list1, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

# Creating list to append tweet data to
tweets_list2 = []

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper('NFT lang:en since:2022-01-01 until:2022-01-02').get_items()):
    if i>1000:
        break
    tweets_list2.append([tweet.date, tweet.id, tweet.content,
                         tweet.user.username, tweet.hashtags,
                         tweet.inReplyToTweetId,tweet.likeCount,
                         tweet.retweetCount])
    
# Creating a dataframe from the tweets list above
tweets_df2 = pd.DataFrame(tweets_list2, columns=[
    'Datetime', 'TweetId', 'Text', 'Username', 'Hashtags','reply_TweetId',
    'likes','retweets'])

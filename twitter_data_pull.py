# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 18:08:55 2022

@author: atishakov
"""
import GetOldTweets3 as got
import twint
import nest_asyncio
import tweepy
nest_asyncio.apply()

# c = twint.Config()

# c.Username = "noneprivacy"
# c.Custom["tweet"] = ["id"]
# c.Custom["user"] = ["bio"]
# c.Limit = 10
# c.Store_csv = True
# c.Output = "none"

# twint.run.Search(c)

tweetCriteria = got.manager.TweetCriteria().setUsername("barackobama")\
                                           .setTopTweets(True)\
                                           .setMaxTweets(10)
tweet = got.manager.TweetManager.getTweets(tweetCriteria)[0]
print(tweet.text)

import pandas as pd
import numpy as np
from db_events_pull import pull_events,pull_tweets
import psycopg2
from sqlalchemy import create_engine
import datetime

pd.options.display.float_format = '{:.4f}'.format

def nft_sales(table):
    # =============================================================================
    # Import data
    # =============================================================================
    # NFT
    nft_cols = ['collection_slug','asset_token_id','event_type',
                'event_timestamp','sampling','bid_amount','total_price',
                'seller_address','winner_account_address','listing_time']
    df = pull_events(nft_cols,table) # makes a call to the database
    # =============================================================================
    # Preparing data
    # =============================================================================
    # NFT
    nft_col_types = {'collection_slug':str,
                     'asset_token_id':str,
                     'event_type':str,
                     'event_timestamp':str,
                     'sampling':str,
                     'bid_amount':str,
                     'total_price':float,
                     'seller_address':str,
                     'winner_account_address':str,
                     'listing_time':str
                     # 'from_account_address':str
                     }
    
    df_nft_sales = df.replace('nan','').astype(nft_col_types) #replace nan with empty string and convert column types
    df_nft_sales = df_nft_sales.rename(columns={"total_price":"total_price_wei"}) #rename to total_price_wei, to convert values to eth
    df_nft_sales['event_timestamp'] = pd.to_datetime(df_nft_sales['event_timestamp']) # convert to date
    df_nft_sales['listing_time'] = pd.to_datetime(df_nft_sales['listing_time']) # convert to date
    df_nft_sales["total_price_eth"] = df_nft_sales["total_price_wei"].div(10**18) # create column for wei -> eth

    return df_nft_sales


def twitter_data():
    # # Tweets
    # tweet_cols = ['TweetId','Datetime', 'Text', 'search_term',
    #                 'Username','reply_TweetId',
    #                 'likes', 'retweets','Hashtags']
    # df_tweet= pd.read_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\all tweets adj.csv')
    # df_tweet = df_tweet[tweet_cols]
    # df_tweets = df_tweet.copy()

    # df_tweet_more = pd.read_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\more_tweets.csv',low_memory=False)
    # df_tweet_more_edit = df_tweet_more.assign(value=pd.to_numeric(df_tweet_more['likes'],errors='coerce')).dropna(subset=['value'])
    # df_tweet_more_edit = df_tweet_more_edit[tweet_cols]
    # # Twitter users
    # twitter_user_cols = ['id', 'username', 'verified','followers_count',
    #                       'following_count', 'statuses_count','favourites_count',
    #                       'raw_description', 'rendered_description','description_links',
    #                       'user_creation_date', 'location','protected','links',
    #                       'user_label']
    # df_tw_user = pd.read_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\all_user_data_combined.csv")
    # df_tw_user = df_tw_user[twitter_user_cols]
    # df_tw_users = df_tw_user.copy()
    # df_tw_user_more = pd.read_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\more_user_data.csv")
    # df_tw_user_more = df_tw_user_more[twitter_user_cols]
    # df_tw_users_more = df_tw_user_more.copy()

    # # Twitter tweets
    # tweet_col_types = {'TweetId':str,
    #                       'Datetime':str,
    #                       'Text':str,
    #                       'search_term':str,
    #                       'Username':str,
    #                       'reply_TweetId':str,
    #                       'likes':float,
    #                       'retweets':float,
    #                       'Hashtags':str}
    # df_tweets = df_tweets.replace('nan','').astype(tweet_col_types) #replace nan with empty string and convert column types
    # df_tweet_more_edit = df_tweet_more_edit.astype(tweet_col_types) #replace nan with empty string and convert column types

    # # Merge all tweets
    # df_tweets = pd.concat([df_tweets,df_tweet_more_edit])


    # # Twitter users
    # twitter_user_col_types = {'id':str,
    #                           'username':str,
    #                           'verified':bool,
    #                           'followers_count':float,
    #                           'following_count':float,
    #                           'statuses_count':float,
    #                           'favourites_count':float,
    #                           'raw_description':str,
    #                           'rendered_description':str,
    #                           'description_links':str,
    #                           'user_creation_date':str,
    #                           'location':str,
    #                           'protected':bool,
    #                           'links':str,
    #                           'user_label':str}
    # df_tw_users = df_tw_users.replace('nan','').astype(twitter_user_col_types) #replace nan with empty string and convert column types
    # df_tw_users_more = df_tw_user_more.replace('nan','').astype(twitter_user_col_types)

    # # Merge all users
    # df_tw_users = pd.concat([df_tw_users,df_tw_users_more])
    # df_tw_users = df_tw_users.astype(twitter_user_col_types)

    # # Merge data about tweets and user data
    # df_tweets.columns = df_tweet.columns.str.lower()
    # tweet_cols = {}
    # tw_user_cols = {}
    # for col_name in df_tweets.columns:
    #     new_name = col_name
    #     if not 'tweet' in col_name:
    #         if not 'username' in col_name:
    #             new_name = 'tweet_'+col_name
    #     tweet_cols[col_name]=new_name
    # for col_name in df_tw_users.columns:
    #     new_name = col_name
    #     if not 'user' in col_name:
    #         new_name = 'user_'+col_name
    #     tw_user_cols[col_name]=new_name
    # df_tweets.rename(columns=tweet_cols,inplace=True)
    # df_tw_users.rename(columns=tw_user_cols,inplace=True)

    # df_tw_users.username = df_tw_users.username.astype('str')
    # df_tweets.username = df_tweets.username.astype('str')
    # df_tw_users.username = df_tw_users.username.str.lower().str.strip()
    # df_tweets.username = df_tweets.username.str.lower().str.strip()


    # df_user_tweets = pd.merge(df_tweets,df_tw_users,left_on=['username'],right_on=['username'],how='outer')
    # df_user_tweets = df_user_tweets.replace('nan','').drop_duplicates(subset=['tweetid']) #replace nan with empty string
    # df_user_tweets['tweet_datetime'] = pd.to_datetime(df_user_tweets['tweet_datetime']) # convert to date
    # df_user_tweets['user_creation_date'] = pd.to_datetime(df_user_tweets['user_creation_date']) # convert to date

    # name_to_slug_df = pd.read_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\search_terms_to_standardise.csv")
    # name_to_slug_dict = pd.Series(name_to_slug_df.tweet_search_term.values,
    #                               index=name_to_slug_df.twitter_handle.values).to_dict()
    # df_user_tweets["tweet_search_term"] = df_user_tweets["tweet_search_term"].replace(name_to_slug_dict)


    # conn = psycopg2.connect(host="localhost",database="nft_collections",user="postgres",password="1")
    # engine = create_engine('postgresql://postgres:1@localhost:5432/nft_collections')

    # conn.autocommit = True

    # df_upload = df_user_tweets.copy()
    # sql_cols = df_upload.columns
    # for cols in df_upload:
    #     df_upload[cols] = df_upload[cols].astype(str).copy()
    # df_upload = df_upload.reindex(df_upload.columns.union(sql_cols, sort=False), axis=1, fill_value='')

    # cols = " varchar(1000), ".join(name for name in sql_cols)
    # cols = cols +" varchar(1000)"
    # df_upload.to_sql(name="tweets",con=engine,if_exists='append',index=False)

    # conn.commit()
    # conn.close()

    # # df_empty_userid = df_user_tweets[df_user_tweets['user_id'].isnull()]
    # # df_slugs = df_user_tweets.groupby('tweet_search_term').describe()

    # # print("\nDone!")

    # =============================================================================
    # Import data
    # =============================================================================
    # Twitter
    original_tweet_len = 1603692
    tweet_cols = ['tweetid',
                'tweet_datetime',
                'tweet_text',
                'tweet_search_term',
                'username',
                'reply_tweetid',
                'tweet_likes',
                'retweets',
                'tweet_hashtags',
                'user_id',
                'user_verified',
                'user_followers_count',
                'user_following_count',
                'user_statuses_count',
                'user_favourites_count',
                'user_raw_description',
                'user_rendered_description',
                'user_description_links',
                'user_creation_date',
                'user_location',
                'user_protected',
                'user_links',
                'user_label']
    df_user_tweets = pull_tweets(tweet_cols,'tweets') # makes a call to the database
    # =============================================================================
    # Preparing data
    # =============================================================================
    # Tweets
    twitter_col_types = {'tweetid':str,
                        'tweet_datetime':str,
                        'tweet_text':str,
                        'tweet_search_term':str,
                        'username':str,
                        'reply_tweetid':str,
                        'tweet_likes':float,
                        'retweets':float,
                        'tweet_hashtags':str,
                        'user_id':str,
                        'user_verified':str,
                        'user_followers_count':float,
                        'user_following_count':float,
                        'user_statuses_count':float,
                        'user_favourites_count':float,
                        'user_raw_description':str,
                        'user_rendered_description':str,
                        'user_description_links':str,
                        'user_creation_date':str,
                        'user_location':str,
                        'user_protected':bool,
                        'user_links':str,
                        'user_label':str}

    df_user_tweets = df_user_tweets[~df_user_tweets['user_creation_date'].isin(['NaT'])]
    df_user_tweets = df_user_tweets.replace('nan','').astype(twitter_col_types) #replace nan with empty string and convert column types
    df_user_tweets['tweet_datetime'] = pd.to_datetime(df_user_tweets['tweet_datetime']).dt.date # convert to date
    df_user_tweets['user_creation_date'] = pd.to_datetime(df_user_tweets['user_creation_date']).dt.date # convert to date
    df_user_tweets = df_user_tweets[(df_user_tweets['user_creation_date'] < datetime.datetime.strptime('2022-01-01',"%Y-%m-%d").date())] # convert to date


    clean_tweet_len = len(df_user_tweets)
    removed_tweet_len = original_tweet_len - clean_tweet_len
    print("Imported "+str(f'{clean_tweet_len:,}')+" tweets.")
    print("Removed "+str(f'{removed_tweet_len:,}')+" tweets.")

    return df_user_tweets

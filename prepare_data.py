import pandas as pd
from db_events_pull import pull_events as pull_events

pd.options.display.float_format = '{:.4f}'.format

def nft_sales(table):
    # =============================================================================
    # Import data
    # =============================================================================
    # NFT
    nft_cols = ['collection_slug','asset_token_id','event_type',
                'event_timestamp','sampling','bid_amount','total_price',
                'from_account_address']
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
                     'from_account_address':str}
    
    df_nft_sales = df.replace('nan','').astype(nft_col_types) #replace nan with empty string and convert column types
    df_nft_sales = df_nft_sales.rename(columns={"total_price":"total_price_wei"}) #rename to total_price_wei, to convert values to eth
    df_nft_sales['event_timestamp'] = pd.to_datetime(df_nft_sales['event_timestamp']) # convert to date
    df_nft_sales["total_price_eth"] = df_nft_sales["total_price_wei"].div(10**18) # create column for wei -> eth

    return df_nft_sales


def twitter_data():
    # Tweets
    tweet_cols = ['TweetId','Datetime', 'Text', 'search_term',
                    'Username','reply_TweetId',
                    'likes', 'retweets','Hashtags']
    df_tweet= pd.read_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\all tweets adj.csv')
    df_tweet = df_tweet[tweet_cols]
    df_tweets = df_tweet.copy()
    
    # Twitter users
    twitter_user_cols = ['id', 'username', 'verified','followers_count',
                         'following_count', 'statuses_count','favourites_count',
                         'raw_description', 'rendered_description','description_links',
                         'user_creation_date', 'location','protected','links',
                         'user_label']
    df_tw_user = pd.read_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\all_user_data_combined.csv")
    df_tw_user = df_tw_user[twitter_user_cols]
    df_tw_users = df_tw_user.copy()
    
    
    # Twitter tweets
    tweet_col_types = {'TweetId':str,
                         'Datetime':str,
                         'Text':str,
                         'search_term':str,
                         'Username':str,
                         'reply_TweetId':str,
                         'likes':int,
                         'retweets':int,
                         'Hashtags':str}
    df_tweets = df_tweets.replace('nan','').astype(tweet_col_types) #replace nan with empty string and convert column types

    # Twitter users
    twitter_user_col_types = {'id':str,
                              'username':str,
                              'verified':bool,
                              'followers_count':int,
                              'following_count':int,
                              'statuses_count':int,
                              'favourites_count':int,
                              'raw_description':str,
                              'rendered_description':str,
                              'description_links':str,
                              'user_creation_date':str,
                              'location':str,
                              'protected':bool,
                              'links':str,
                              'user_label':str}
    df_tw_users = df_tw_users.replace('nan','').astype(twitter_user_col_types) #replace nan with empty string and convert column types

    # Merge data about tweets and user data
    df_tweets.columns = df_tweet.columns.str.lower()
    tweet_cols = {}
    tw_user_cols = {}
    for col_name in df_tweets.columns:
        new_name = col_name
        if not 'tweet' in col_name:
            if not 'username' in col_name:
                new_name = 'tweet_'+col_name
        tweet_cols[col_name]=new_name
    for col_name in df_tw_users.columns:
        new_name = col_name
        if not 'user' in col_name:
            new_name = 'user_'+col_name
        tw_user_cols[col_name]=new_name
    df_tweets.rename(columns=tweet_cols,inplace=True)
    df_tw_users.rename(columns=tw_user_cols,inplace=True)
    
    df_user_tweets = pd.merge(df_tweets,df_tw_users,on=['username'])
    df_user_tweets = df_user_tweets.replace('nan','').drop_duplicates(subset=['tweetid']) #replace nan with empty string
    df_user_tweets['tweet_datetime'] = pd.to_datetime(df_user_tweets['tweet_datetime']) # convert to date
    df_user_tweets['user_creation_date'] = pd.to_datetime(df_user_tweets['user_creation_date']) # convert to date

    return df_user_tweets

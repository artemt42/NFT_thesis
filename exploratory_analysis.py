import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sklearn
from prepare_data import nft_sales,twitter_data
import financialanalysis as fa
from collections import defaultdict
import statsmodels.api as sm
import os
from datetime import datetime
import emoji
from textblob import TextBlob

pd.options.display.float_format = '{:.4f}'.format

# =============================================================================
# Read data
# =============================================================================

df1 = nft_sales('nft_sale_events')
df2 = twitter_data()

df_nft = df1.copy()
# =============================================================================
# collection_slug,asset_token_id,event_type,event_timestamp,sampling,bid_amount,
# total_price_wei,from_account_address,total_price_eth
# =============================================================================
df_twitter = df2.copy()
# =============================================================================
# tweetid,tweet_datetime,tweet_text,tweet_search_term,username,reply_tweetid,
# tweet_likes,retweets,tweet_hashtags
# user_id,user_verified,user_followers_count,user_following_count,
# user_statuses_count,user_favourites_count,user_raw_description,
# user_rendered_description,user_description_links, user_creation_date,
# user_location,user_protected,user_links,user_label
# =============================================================================

# t = df_twitter[(df_twitter['user_protected']).isna()].groupby('username').agg('count').sort_values(by='tweetid',ascending=False)
# t['tweetid'].to_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\missing_user_data_more.csv')

# =============================================================================
# Analysis
# =============================================================================

def empty_dates(df_temp,date,key):
    # Take the diff of the first column (drop 1st row since it's undefined)
    deltas = df_temp[date].diff()[1:]
    
    # Filter diffs (here days > 1, but could be seconds, hours, etc)
    gaps = deltas[deltas > timedelta(days=1)]
    if not gaps.empty:
        print(key)
    # Print results
        print(f'{len(gaps)} gaps with average gap duration: {gaps.mean()}')
        for i, g in gaps.iteritems():
            gap_start = df_temp[date][i - 1]
            print(f'Start: {datetime.strftime(gap_start, "%Y-%m-%d")} | '
                  f'Duration: {str(g.to_pytimedelta())}')

#NFT
df_group_slug = df_nft.groupby('collection_slug')
df_slug_summary = df_group_slug.total_price_eth.agg(['count','min', 'max', 'median', 'mean']).round(4)


def nft_grouper(df_nft):

    df_temp = defaultdict(dict) # all daily nft averages
    df_slug_prices = defaultdict(dict) # all daily nft averages

    df_nft_sales = df_nft[['collection_slug','event_timestamp','total_price_eth']].copy()
    df_nft_sales['date'] = df_nft_sales['event_timestamp'].dt.normalize()
    df_nft_sales = df_nft_sales.transpose().to_dict()

    for index_,nfts_df in df_nft_sales.items():
        coll_slug = nfts_df['collection_slug']
        df_temp[coll_slug][index_] = {'date':nfts_df['date'],
                                      'total_price_eth':nfts_df['total_price_eth']}

    for key,value in df_temp.items():
        df_slug = pd.DataFrame(value).transpose()
        df_slug['date'] = pd.to_datetime(df_slug['date']).dt.normalize()
        df_slug['total_price_eth'] = df_slug['total_price_eth'].astype(float)
        df_slug_prices[key] = df_slug

    return df_slug_prices

def get_tweet_sent_result(score):
    # set sentiment
    if score > 0:
        result = 'positive'
    elif score == 0:
        result = 'neutral'
    else:
        result = 'negative'

    return result

def emoji_to_text(text):
    if type(text) != float:
        return emoji.demojize(text,language='en')
    else:
        return text


# "Testing"

# df_tweets_terms = df_twitter[['tweetid','tweet_search_term']].groupby('tweet_search_term').agg(['count'])
# df_tweets_popular_users_summary =df_tweets_popular_users['user_followers_count'].agg(['count','min', 'max', 'median', 'mean']).round(4)
# df_tweets_group_term_summaryRT =df_tweets_group_term['retweets'].agg(['count','min', 'max', 'median', 'mean']).round(4)

def twitter_grouper(min_likes,min_retw,min_term,tw_term,df_tw_clean):

    def more_data(col_list,day):
        switch = False
        new_col_list = []
        for col in col_list:
            data = day[1].groupby('tweet_search_term')[col].agg(['count','min', 'max', 'median', 'mean'])
            data = data.add_prefix(col+"_")

            if not switch:
                comb_data = data
            else:
                comb_data = pd.merge(comb_data,data,on="tweet_search_term")
            new_col_list += list(data.columns)
            switch = True
        return comb_data,new_col_list

    #Twitter
    name_to_slug_dict = {'10KTF nft':'10ktf','10KTF Stockroom nft':'10ktf-stockroom','adidas Originals Into the Metaverse nft':'adidasoriginals','alien frens nft':'alienfrensnft','Bored Ape Chemistry Club nft':'bored-ape-chemistry-club','Bored Ape Kennel Club nft':'bored-ape-kennel-club','Bored Ape Yacht Club nft':'boredapeyachtclub','CLONE X - X TAKASHI MURAKAMI nft':'clonex','Cool Cats NFT':'cool-cats-nft','CrypToadz by GREMPLIN nft':'cryptoadz-by-gremplin','CryptoPunks nft':'cryptopunks','DeadFellaz nft':'deadfellaz','Decentraland nft':'decentraland','Doodles nft':'doodles-official','Kaiju Kingz nft':'kaiju-kingz','Meebits nft':'meebits','mfers nft':'mfers','Milady Maker nft':'milady','Mutant Ape Yacht Club nft':'mutant-ape-yacht-club','NFT Worlds':'nft-worlds','Cyber Factory 2 nft':'oncyber','PROOF Collective nft':'proof-collective','Psychedelics Anonymous Genesis nft':'psychedelics-anonymous-genesis','Pudgy Penguins nft':'pudgypenguins','The Sandbox nft':'sandbox','TBAC nft':'tbac','VeeFriends nft':'veefriends','World of Women nft':'world-of-women-nft'}

    "Production"
    
    daily_tw_averages = defaultdict(dict) # all daily nft averages
    daily_tw_averages_temp = defaultdict(dict) # all daily nft averages

    df_tw_clean['tweet_datetime'] = pd.to_datetime(df_tw_clean['tweet_datetime'])
    df_tw_dates = df_tw_clean.set_index(['tweet_datetime'])

    daily_averages = {}
    daily_tw = df_tw_dates.groupby(pd.Grouper(freq='D'))
    for day in daily_tw:
        # data1 = day[1].groupby('tweet_search_term')[tw_term].agg(['count','min', 'max', 'median', 'mean'])
        # daily_averages_name = day[0].strftime("%Y-%m-%d")
        # data1 = data1.add_prefix(tw_term+"_")

        # data2 = day[1].groupby('tweet_search_term')['tweet_likes'].agg(['count','min', 'max', 'median', 'mean'])
        # data2 = data2.add_prefix(t)
        tweet_date = day[0]
        data,col_list = more_data(tw_term,day)
        del day
        daily_averages_name = tweet_date.strftime("%Y-%m-%d")
        daily_averages[daily_averages_name] = data
        df_temp = data.transpose().to_dict().copy()
        for key,value in df_temp.items():
            if key in name_to_slug_dict:
                key = name_to_slug_dict[key]
                daily_tw_averages_temp[key][daily_averages_name] = value
    
    for key,value in daily_tw_averages_temp.items():
        df_temp = pd.DataFrame(value).transpose()
        df_temp.index.name = 'tweet_datetime'
        df_temp = df_temp.reset_index()
        df_temp['tweet_datetime'] = pd.to_datetime(df_temp['tweet_datetime']) # convert to date and filter for june only sales
        idx = pd.date_range(df_temp['tweet_datetime'].min(), df_temp['tweet_datetime'].max())
        # empty_dates(df_temp,'tweet_datetime',key) #check for dates with no data
        df_temp.set_index('tweet_datetime',inplace=True)
        df_temp = df_temp.asfreq('D',fill_value=np.nan)
        df_temp = df_temp.reindex(idx, fill_value=0)
        df_temp = df_temp[df_temp.index >'2021-05-31']
    
        # df_temp['interpolated_count'] = df_temp['count'].interpolate(option='spline')
        # analysis_col = 'count'
        # new_tw_col_name = tw_term+"_"+analysis_col
        # df_temp = df_temp.rename(columns={'count':new_tw_col_name})
        daily_tw_averages[key] = df_temp

    return daily_tw_averages, col_list
    # del daily_tw,day,daily_averages_name,daily_tw_averages_temp,df_temp,analysis_col,idx

def analyse_grouper(col_list,nft_values_per_slug,df_twitter_clean):
    #Combining the data
    df_nft_analyse = defaultdict(dict)
    # df_nft_analyse = daily_nft_values.copy()
    nft_variable = 'total_price_eth'
    lag = -1

    for key,value in nft_values_per_slug.items():
        # value = value.reset_index()
        # value['interpolated_mean'] = value['mean'].interpolate(option='spline')
        # value = value.set_index('date')
        df_nft_analyse[key] = value[['event_timestamp',nft_variable]]#[:lag]

    df_tw_analyse = defaultdict(dict)
    for key,value in df_twitter_clean.items():
        value = value.reset_index()
        value = value.rename(columns={'index':'date'})
        df_tw_analyse[key] = value
    
    df_analyse = defaultdict(dict)
    for key,value in df_tw_analyse.items():
        df_temp = value.merge(df_nft_analyse[key],on='date',how='left')
        # df_temp['rel_price_change_pct'] = df_temp['interpolated_mean'].pct_change()
        # df_temp['rel_price_change_pct'] = df_temp['rel_price_change_pct'].fillna(1)
        df_temp[nft_variable] = df_temp[nft_variable].shift(lag)
        default_cols = ["date",nft_variable,"user_followers_count_mean"]
        df_analyse[key] = df_temp[default_cols].dropna()

    return df_analyse,default_cols

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

#Cleaning data
# df_tw = df_twitter.copy()

#basic operations
df_twitter['tweet_len'] = df_twitter['tweet_text'].str.len().copy()


# text cleaning
# exclude_usernames = ['StickyDAO','nft_bot_sniper','TpunksB','CoolCatsNFTBot',
#                      'alphabettyBot','0xbotfather','DF_Marketbot','nftsalesbot',
#                      'bot_cryptopunks','ETHBot','boredapebot','nftgobot',
#                      'CryptoStatsBot','NftsRankingBot','nftsbot','BottleheadsNFT_',
#                      'DoodleBotOS','dclwearablesbot','PunkBabyBot','Khldon_bot',
#                      'tsukimitech_bot','cryptoadzBot','dclnamesbot','NewsCryptoBot',
#                      'SuperRareBot','Frankenbot2000','BigDealNFTBot','cryptomaticbot',
#                      'ButterLemonBot','dcllandbot','BearsDeluxeSales']
# df_tw_clean = df_tw_clean[~df_tw_clean['username'].isin(exclude_usernames)]


df_twitter['tweet_text_no_emoji'] = df_twitter['tweet_text'].apply(emoji_to_text)
df_twitter['tweet_clean'] = df_twitter['tweet_text_no_emoji'].str.replace("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)",'',regex=True)
df_twitter['tweet_clean'] = df_twitter['tweet_clean'].str.strip()
df_twitter = df_twitter[df_twitter['tweet_clean'].str.len() > 0]
exclude_tweet_strings = ['0 ']
df_twitter = df_twitter[~df_twitter['tweet_clean'].isin(exclude_tweet_strings)]

# text sentiment analysis
df_twitter['tweet_sent_score'] = df_twitter['tweet_clean'].apply(sentiment_calc)
df_twitter['tweet_sent_polarity'] = df_twitter['tweet_sent_score'].apply(lambda x:get_tweet_sent_result(x))
df_tw_clean = df_twitter.copy()

# df_tw_clean.to_csv(r"C:\Users\atish\Documents\GitHub\NFT_thesis\twitter_users\clean csv\tweets.csv")



min_likes = 5
min_retw = 0
min_followers = 100
analysis_columns = ['user_followers_count']

# d = df_twitter.groupby('tweet_search_term').describe()
# d1 = df_nft.groupby('collection_slug').describe()
# d.to_csv(r"G:\My Drive\BIM Thesis\results\twitter_stats.txt")
# d1.to_csv(r"G:\My Drive\BIM Thesis\results\nft_stats.txt")


# Analysis
# Popularity
# df_tw_clean = df_tw_clean[(df_twitter['retweets']>=min_likes) &
#                           (df_twitter['tweet_likes']>=min_retw) &
#                           (df_twitter['user_followers_count']>=min_followers)]

nft_values_per_slug = nft_grouper(df_nft.copy())

# Sentiment
df_tw_clean = df_tw_clean[(df_tw_clean['tweet_sent_score']!=0)]

df_twitter_clean,col_list = twitter_grouper(min_likes,min_retw,min_followers,analysis_columns,df_tw_clean)

# nft_variable = 'interpolated_mean'
# nft_variable = 'rel_price_change_pct'
df_analyse,default_cols = analyse_grouper(col_list,nft_values_per_slug,df_twitter_clean)
# default_cols.remove('date')


ts = datetime.now().strftime("%Y%m%d %H%M%S")
# Model

def results_summary_to_dataframe(nft,results):
    '''take the result of an statsmodel results table and transforms it into a dataframe'''
    pvals = results.pvalues
    coeff = results.params
    conf_lower = results.conf_int()[0]
    conf_higher = results.conf_int()[1]
    rsquared = results.rsquared
    rsquared_adj = results.rsquared_adj
    num_observations = len(results.fittedvalues)

    results_dict = pd.DataFrame({"nft":nft,
                    "num_observations":num_observations,
                    "coeff":coeff,
                    "pvals":pvals,
                    "conf_lower":conf_lower,
                    "conf_higher":conf_higher,
                    "rsquared":rsquared,
                    "rsquared_adj":rsquared_adj,
                     })
    return results_dict

# tmp_list = []
# for nft,value in df_analyse.items():
#     if len(value) > 20: # any less than that is statistically insignificant
#         # nft = 'cool-cats-nft'
#         X = df_analyse[nft][default_cols]
#         y = df_analyse[nft][nft_variable]
#         X = sm.add_constant(X) ## let's add an intercept (beta_0) to our model
#         model = sm.OLS(y,X.astype(float)).fit()
#         predictions = model.predict(X)
#         model_summary = model.summary()
#         tmp_list.append(results_summary_to_dataframe(nft,model))


# analysis_var = '_'.join(analysis_columns)
# results_df = pd.concat(tmp_list)
# results_df.to_csv(r'C:\Users\atish\Documents\GitHub\NFT_thesis\results\results-'+ts+'-'+analysis_var+".csv")

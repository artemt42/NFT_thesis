import pandas as pd
import numpy as np
import matplotlib as mpl
from datetime import datetime, timedelta
import sklearn
from prepare_data import nft_sales,twitter_data
import financialanalysis as fa
from collections import defaultdict
import statsmodels.api as sm

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
df_nft.groupby('event_timestamp')

nft_slugs = []

daily_averages = {}
daily_nft_averages = defaultdict(dict) # all daily nft averages
daily_nft_averages_temp = defaultdict(dict) # all daily nft averages

df_nft_sales_dates = df_nft.set_index(['event_timestamp'])
daily_sales = df_nft_sales_dates.groupby(pd.Grouper(freq='D'))
for day in daily_sales:
    data = day[1].groupby('collection_slug')['total_price_eth'].agg(['count','min', 'max', 'median', 'mean']).copy()
    daily_averages_name = day[0].strftime("%Y-%m-%d")
    daily_averages[daily_averages_name] = data
    df_temp = data.transpose().to_dict().copy()
    for key,value in df_temp.items():
        daily_nft_averages_temp[key][daily_averages_name] = value

for key,value in daily_nft_averages_temp.items():
    df_temp = pd.DataFrame(value).transpose()
    df_temp.index.name = 'date'
    df_temp = df_temp.reset_index()
    df_temp['date'] = pd.to_datetime(df_temp['date']) # convert to date and filter for june only sales
    idx = pd.date_range(df_temp['date'].min(), df_temp['date'].max())
    # empty_dates(df_temp,'date',key) #check for dates with no data
    df_temp.set_index('date',inplace=True)
    df_temp = df_temp.asfreq('D',fill_value=np.nan)
    # df_temp = df_temp.reindex(idx, fill_value=0)
    # df_temp.drop(['date'])
    df_temp = df_temp.sort_values(by = ['date'])
    df_temp = df_temp[df_temp.index >'2021-05-31']
    df_temp['interpolated_mean'] = df_temp['mean'].interpolate(option='spline')
    df_temp['rel_price_change_pct'] = df_temp['interpolated_mean'].pct_change()
    df_temp['rel_price_change_pct'] = df_temp['rel_price_change_pct'].fillna(1)
    nft_slugs.append(key)
    daily_nft_averages[key] = df_temp

del daily_sales,day,daily_averages_name,daily_nft_averages_temp,df_temp

name_to_slug_dict = {'10KTF nft':'10ktf','10KTF Stockroom nft':'10ktf-stockroom','adidas Originals Into the Metaverse nft':'adidasoriginals','alien frens nft':'alienfrensnft','Bored Ape Chemistry Club nft':'bored-ape-chemistry-club','Bored Ape Kennel Club nft':'bored-ape-kennel-club','Bored Ape Yacht Club nft':'boredapeyachtclub','CLONE X - X TAKASHI MURAKAMI nft':'clonex','Cool Cats NFT':'cool-cats-nft','CrypToadz by GREMPLIN nft':'cryptoadz-by-gremplin','CryptoPunks nft':'cryptopunks','DeadFellaz nft':'deadfellaz','Decentraland nft':'decentraland','Doodles nft':'doodles-official','Kaiju Kingz nft':'kaiju-kingz','Meebits nft':'meebits','mfers nft':'mfers','Milady Maker nft':'milady','Mutant Ape Yacht Club nft':'mutant-ape-yacht-club','NFT Worlds':'nft-worlds','Cyber Factory 2 nft':'oncyber','PROOF Collective nft':'proof-collective','Psychedelics Anonymous Genesis nft':'psychedelics-anonymous-genesis','Pudgy Penguins nft':'pudgypenguins','The Sandbox nft':'sandbox','TBAC nft':'tbac','VeeFriends nft':'veefriends','World of Women nft':'world-of-women-nft'}

#Twitter
"Testing"

df_tweets_popular_users = df_twitter[df_twitter['user_followers_count']>1000].groupby('tweet_search_term')
df_tweets_popular_users_summary =df_tweets_popular_users['user_followers_count'].agg(['count','min', 'max', 'median', 'mean']).round(4)
# df_tweets_group_term_summaryRT =df_tweets_group_term['retweets'].agg(['count','min', 'max', 'median', 'mean']).round(4)


"Production"
tw_term = 'user_followers_count'
daily_tw_averages = defaultdict(dict) # all daily nft averages
daily_tw_averages_temp = defaultdict(dict) # all daily nft averages

df_tw_dates = df_twitter[df_twitter[tw_term]>1000].set_index(['tweet_datetime'])
daily_tw = df_tw_dates.groupby(pd.Grouper(freq='D'))
for day in daily_tw:
    data = day[1].groupby('tweet_search_term')[tw_term].agg(['count','min', 'max', 'median', 'mean']).copy()
    daily_averages_name = day[0].strftime("%Y-%m-%d")
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
    df_temp['interpolated_mean'] = df_temp['mean'].interpolate(option='spline')
    new_tw_col_name = tw_term+"_interpolated_mean"
    df_temp = df_temp.rename(columns={'interpolated_mean':new_tw_col_name})
    # df_temp['rel_price_change_pct'] = df_temp['ewa'].pct_change()
    # df_temp['rel_price_change_pct'] = df_temp['rel_price_change_pct'].fillna(1)
    daily_tw_averages[key] = df_temp

# del daily_tw,day,daily_averages_name,daily_tw_averages_temp,df_temp

#Combining the data
df_nft_analyse = defaultdict(dict)
for key,value in daily_nft_averages.items():
    value = value.reset_index()
    value['rel_price_change_pct'] = value['rel_price_change_pct'].shift(-1)
    df_nft_analyse[key] = value[['date','rel_price_change_pct']][:-1]

df_tw_analyse = defaultdict(dict)
for key,value in daily_tw_averages.items():
    value = value.reset_index()
    value = value.rename(columns={'index':'date'})
    df_tw_analyse[key] = value[['date',new_tw_col_name]]

df_combined_analyse = defaultdict(dict)
for key,value in df_tw_analyse.items():
    f = value.merge(df_nft_analyse[key],on='date',how='inner')
    df_combined_analyse[key] = f

nft = 'meebits'
X = df_combined_analyse[nft]['user_followers_count_interpolated_mean']
y = df_combined_analyse[nft]['rel_price_change_pct']
# X = sm.add_constant(X) ## let's add an intercept (beta_0) to our model

model = sm.OLS(y,X).fit()
predictions = model.predict(X)

model.summary()

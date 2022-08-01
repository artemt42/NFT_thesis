library ('dplyr')
# library ('plyr')
library ('stats')
library ('broom')
library('tidyr')
options(scipen=999)

# ---- Tweet import and format ----
source_tweets <- read.csv("C:\\Users\\atish\\Documents\\GitHub\\NFT_thesis\\rstudio\\tweets_clean_rstudio.csv",
                          header=TRUE)

subset_tweets = subset(source_tweets,select = -c(tweet_text,reply_tweetid,tweet_hashtags,user_raw_description,user_description_links,user_location,user_protected,user_links,user_label,tweet_sent_language))


subset_tweets$tweetid <- as.character(subset_tweets$tweetid)
subset_tweets$username <- as.character(subset_tweets$username)
subset_tweets$user_id <- as.character(subset_tweets$user_id)
subset_tweets$tweet_datetime <- as.Date(subset_tweets$tweet_datetime)
subset_tweets$user_creation_date <- as.Date(subset_tweets$user_creation_date)
subset_tweets$user_verified <- as.logical(subset_tweets$user_verified)
subset_tweets$tweet_likes <- as.integer(subset_tweets$tweet_likes)
subset_tweets$retweets <- as.integer(subset_tweets$retweets)
subset_tweets$user_followers_count <- as.integer(subset_tweets$user_followers_count)
subset_tweets$user_following_count <- as.integer(subset_tweets$user_following_count)
subset_tweets$user_statuses_count <- as.integer(subset_tweets$user_statuses_count)
subset_tweets$user_favourites_count <- as.integer(subset_tweets$user_favourites_count)

subset_tweets = subset_tweets %>% filter(tweet_datetime > '2021-05-30')

names(subset_tweets)[3] <- 'datetime'
names(subset_tweets)[4] <- 'collection_slug'

rm(source_tweets)

# ---- NFT import and format ----

source_nfts <- read.csv("C:\\Users\\atish\\Documents\\GitHub\\NFT_thesis\\rstudio\\nfts_clean.csv",
                        header=TRUE)

subset_nfts = subset(source_nfts,select = -c(sampling,bid_amount,total_price_wei))
rm(source_nfts)

subset_nfts$asset_token_id <- as.character(subset_nfts$asset_token_id)
subset_nfts$from_account_address <- as.character(subset_nfts$from_account_address)
subset_nfts$event_timestamp <- as.Date(subset_nfts$event_timestamp)


names(subset_nfts)[4] <- 'datetime'

# ---- Groupby functions ----

# subset_tweets

foll = 0
likes = 0
retw = 0

tweet_group_day_slug =
  subset_tweets %>%
  dplyr::group_by(datetime,collection_slug) %>%
  dplyr::filter(user_followers_count >= foll & tweet_likes >= likes & retweets >= retw ) %>%
  dplyr::summarise(
            total_tweets = n(),
            average_likes = round(mean(tweet_likes),5),
            average_retweets = round(mean(retweets),5),
            average_followers = round(mean(user_followers_count),5),
            average_sent = round(mean(tweet_sent_score),5),
            .groups='drop')



# subset_nfts

lag_days = 1
subset_nfts$datetime <- lag(subset_nfts$datetime,lag_days)


#---- Analysis ----

df_combined_left <- merge(x = tweet_group_day_slug ,y = subset_nfts,how='left', by=c("datetime", "collection_slug"))

nft_price <- df_combined_left$total_price_eth
tweet_mean_followers <- df_combined_left$average_followers
tweet_count <- df_combined_left$total_tweets
tweet_follower.tweet_count <- tweet_mean_followers * tweet_count

# summary(lm(nft_price ~ 
#              tweet_mean_followers +
#              tweet_count +
#              tweet_follower.tweet_count))

models <- 
  df_combined_left %>% 
  nest_by(collection_slug) %>% 
  mutate(mod=list(lm(nft_price ~
          tweet_mean_followers +
          tweet_count +
          tweet_follower.tweet_count,
          data = data))) %>%
  summarise(tidy(mod))


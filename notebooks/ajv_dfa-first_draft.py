# -*- coding: utf-8 -*-
# # Retrieving data from Twitter

from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv_dfa-first_draft')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

import logging
import pandas as pd
import tweepy
import yaml

twitter_keys = utils.load_config()['default']['twitter']


def connect_to_twitter_api(wait_on_rate_limit=False):
    
    twitter_keys = utils.load_config()['default']['twitter']
    
    auth = tweepy.OAuthHandler(twitter_keys['consumer_key'], 
                               twitter_keys['consumer_secret'])
    
    auth.set_access_token(twitter_keys['access_token_key'], 
                          twitter_keys['access_token_secret'])
    
    return tweepy.API(auth, wait_on_rate_limit=wait_on_rate_limit)


def retrieve_tweets_from_user(username, number_of_tweets=100, wait_on_rate_limit=False, max_id=None,
                             since_id=None):
    
    api = connect_to_twitter_api(wait_on_rate_limit)
        
    tweets = []
    for tweet in tweepy.Cursor(api.user_timeline,
                               id=username, 
                               max_id=max_id,
                               since_id=since_id,
                               tweet_mode='extended').items(number_of_tweets):
        tweets.append(tweet._json)
    
    return tweets


def retrieve_tweets_from_hashtag(hashtag, number_of_tweets=100, wait_on_rate_limit=False,max_id=None,
                                 since_id=None):
    
    api = connect_to_twitter_api(wait_on_rate_limit)
    
    tweets = []
    
    for tweet in tweepy.Cursor(api.search, q=hashtag,
                               max_id=max_id, since_id=since_id,
                               tweet_mode='extended').items(number_of_tweets):
        tweets.append(tweet._json)
    
    return tweets


def retrieve_from_twitter(entity, number_of_tweets=100, wait_on_rate_limit=False, max_id=None,
                         since_id=None):

    if '@' in entity:
        return retrieve_tweets_from_user(entity, number_of_tweets, wait_on_rate_limit, max_id, since_id)
    
    elif '#' in entity:
        return retrieve_tweets_from_hashtag(entity, number_of_tweets, wait_on_rate_limit, max_id, since_id)
    
    print("Should pass a username or hashtag with the proper format (@, #)")


# ## Trials for retrieving tweets

# ### Tweets per user

username = '@dacfortuny'

tweets = retrieve_from_twitter(username, number_of_tweets=50)

len(tweets)

tweets[0]

# ### Tweets per hashtag

hashtag = '#FelizDomingo'

tweets = retrieve_from_twitter(hashtag, 20)

len(tweets)

# ## Parsing JSONs 

json_test = tweets[14]

json_test

json_test.keys()

json_test["user"]

# From raw tweets, we are retrieving the following information separated into 3 tables:
# * **Tweets**: information about the tweets, such as user, creation datetime, source, text, etc. Each row corresponds to a single tweet (whether it is a regular one, a retweet or a quote).
# * **Mentions**: Relation of tweets with mentions. Each tweet has as many rows as different mentions.
# * **Hashtags**: Same as mentions but with hashtags.

# +
l_tweets = []
l_mentions = []
l_hashtags = []

for tweet in tweets:
    
    tweet_keys = tweet.keys()
    tweet_id = tweet['id']
    
    # Define type of tweet
    if 'retweeted_status' in tweet_keys:
        tweet_type = 'rt'
    elif 'quoted_status' in tweet_keys:
        tweet_type = 'quote'
    else:
        tweet_type = 'regular'
    
    tmp_tweets = pd.DataFrame({'tweet_id': [tweet_id],
                               'created_at': [tweet['created_at']],
                               'text': [tweet['full_text']],
                               'source': [tweet['source']],
                               'in_reply_to_status_id': [tweet['in_reply_to_status_id']],
                               'user_id': [tweet['user']['id']],
                               'geo': [tweet['geo']],
                               'coordinates': [tweet['coordinates']],
                               'place': [tweet['place']],
                               'contributors': [tweet['contributors']],
                               'retweet_count': [tweet['retweet_count']],
                               'favorite_count': [tweet['favorite_count']],
                               'favorited': [tweet['favorited']],
                               'retweeted': [tweet['retweeted']],
                               'lang': [tweet['lang']],
                               'type': tweet_type
                              })
    
    # Parse entities in tweet (mentions and hashtags)
    if 'entities' in tweet_keys:
        for mention in tweet['entities']['user_mentions']:   
            tmp_mentions = pd.DataFrame({'tweet_id': [tweet_id],
                                         'user_id': [mention['id']]})
            l_mentions.append(tmp_mentions)
        
        for hashtag in tweet['entities']['hashtags']:
            tmp_hash = pd.DataFrame({'tweet_id': tweet_id,
                                     'hashtag': [hashtag['text']]})
            l_hashtags.append(tmp_hash)
    
    l_tweets.append(tmp_tweets)
# -

df_tweets = pd.concat(l_tweets).reset_index(drop=True)
df_mentions = pd.concat(l_mentions).reset_index(drop=True)
df_hashtags = pd.concat(l_hashtags).reset_index(drop=True)

users = {tweet["user"]["id"]:tweet["user"] for tweet in tweets}

users

COLUMNS_USERS = ["id", "name", "screen_name", "location", "followers_count", "friends_count", "created_at", "favourites_count",
                 "time_zone", "geo_enabled", "verified", "statuses_count", "lang"]
df_users = pd.DataFrame(users.values())[COLUMNS_USERS]
df_users.head()

df_tweets.head()

df_mentions.head()

# +
#df_mentions['id_len'] = df_mentions['user_id'].apply(lambda x: len(str(x)))

# +
#df_mentions.sort_values('id_len', ascending=False).head(20)
# -

df_hashtags.head()

df_hashtags.groupby('tweet_id').agg('count').rename(columns={'hashtag': 'count'}). \
        sort_values('count', ascending=False)

# ### Questions to solve
# 1. Dates
#     * Com estan les dates: UTC o local? 
#     * Sembla que a Twitter t'ensenyen l'hora local.
#     * Com se les descarrega la llibreria?
# 2. Límits (https://developer.twitter.com/en/docs/basics/rate-limiting)
#     * Quantes crides podem fer?
#     * Cada quant?
#     * Per què és diferent per users que per hashtags?
# 3. Tweets
#     * Hi ha 3 tipus de piulades:
#         * Tweet
#         * Retweet
#         * Quote
#     * Es pot accedir a la informació d'un tweet antic per ID?
#     
# ### Next steps
# 1. Netejar el codi per poder-lo posar a córrer
#     - [x] Poder descarregar tweets per username i/o hashtag (= JSON tornat)
#         * Input: username=None, hashtag=None, start_date
#         * Output: JSON per tweet
#         * Research: com es trien els camps? Es pot accedir a un tweet per ID?
#     - [x] Parsejar JSONs i posar-ho en format taules:
#         - [x] Històric tweets
#         - [x] Usuaris únics
#         - [x] Hashtags
#         - [x] Mentions
#     - [ ] Empalmar bé perquè es comencin a baixar des de l'últim.
#     - [ ] Un cop tinguem una mostra de dades, analitzar quins camps són nuls normalment, etc. i acabar decidint quins camps volem.
#     
# 2. Quins hashtags ens interessen? 
#     * Output: omplir el document
#

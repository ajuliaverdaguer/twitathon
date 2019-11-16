# -*- coding: utf-8 -*-
from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv_dfa-first_draft')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

import logging
import tweepy
import twitter
import yaml

twitter_keys = utils.load_config()['default']['twitter']


def get_tweets(username):
    #http://tweepy.readthedocs.org/en/v3.1.0/getting_started.html#api
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)
    api = tweepy.API(auth)
    #set count to however many tweets you want
    number_of_tweets = 100
    #get tweets
    tweets_for_csv = []
    for tweet in tweepy.Cursor(api.user_timeline, screen_name=username).items(number_of_tweets):
        #create array of tweet information: username, tweet id, date/time, text
        tweets_for_csv.append([username, tweet.id_str, tweet.created_at, tweet.text.encode("utf-8")])
    return tweets_for_csv
    #write to a new csv file from the array of tweets


def connect_to_twitter_api(wait_on_rate_limit=False):
    
    twitter_keys = utils.load_config()['default']['twitter']
    
    auth = tweepy.OAuthHandler(twitter_keys['consumer_key'], 
                               twitter_keys['consumer_secret'])
    
    auth.set_access_token(twitter_keys['access_token_key'], 
                          twitter_keys['access_token_secret'])
    
    return tweepy.API(auth, wait_on_rate_limit=wait_on_rate_limit)


def retrieve_tweets_from_user(username):
    api = connect_to_twitter_api()
    
    number_of_tweets = 100
    
    tweets_for_csv = []
    for tweet in tweepy.Cursor(api.user_timeline, screen_name=username).items(number_of_tweets):
        #create array of tweet information: username, tweet id, date/time, text
        tweets_for_csv.append([username, tweet.id_str, tweet.created_at, tweet.text.encode("utf-8")])
    
    return tweets_for_csv


def retrieve_tweets_from_hashtag(hashtag, number_of_tweets=100, wait_on_rate_limit=False):
    api = connect_to_twitter_api(wait_on_rate_limit)
    
    tweets = []
    
    for tweet in tweepy.Cursor(api.search, q=hashtag, count=number_of_tweets,
                              tweet_mode='extended').items():
        tweets.append(tweet)
    
    return tweets


username = '@dacfortuny'

dac = retrieve_tweets_from_user(username)

dac[0]

hashtag = '#PyDayBCN'

pyday = retrieve_tweets_from_hashtag(hashtag)

hashtag = '#MaduixaToLoka'
hashtag = '#OnSonLesMaduixes'
maduixes = retrieve_tweets_from_hashtag(hashtag)

len(maduixes)

maduixes[1]._json

pyday[0]._json

for t in pyday:
    if t.id == 1195635160449863680:
        original_text = t.full_text
        print(t._json)

original_text

pyday[0].text

pyday[0]

hashtag = '#MeToo'
metoo = retrieve_tweets_from_hashtag(hashtag, wait_on_rate_limit=True)

len(metoo)

didac = retrieve_tweets_from_user(username)

len(didac)

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
#     * Poder descarregar tweets per username i/o hashtag (= JSON tornat)
#         * Input: username=None, hashtag=None, start_date
#         * Output: JSON per tweet
#         * Research: com es trien els camps? Es pot accedir a un tweet per ID?
#     * Parsejar JSONs i posar-ho en format taules:
#         * Històric tweets
#         * Usuaris únics
#         * Hashtags
#         * Mentions
#     * Un cop tinguem una mostra de dades, analitzar quins camps són nuls normalment, etc. i acabar decidint quins camps volem.
#     
# 2. Quins hashtags ens interessen? 
#     * Output: omplir el document
#



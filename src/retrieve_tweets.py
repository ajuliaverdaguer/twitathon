"""
Retrieve tweets from data file
"""

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent.parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

import datetime
import fire
import pandas as pd
import time

from tqdm import tqdm

OUTPUT_PATH = "data/raw"

LOG_FILE = f"{OUTPUT_PATH}/log.txt"
MONTH_PREFIX = time.strftime("%Y-%m")


def create_tweets_mentions_hashtags_dataframes(tweets):
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

        tmp_tweets = pd.DataFrame({'tweet_id': [str(tweet_id)],
                                   'created_at': [tweet['created_at']],
                                   'text': [tweet['full_text']],
                                   'source': [tweet['source']],
                                   'in_reply_to_status_id': [str(tweet['in_reply_to_status_id'])],
                                   'user_id': [str(tweet['user']['id'])],
                                   'geo': [tweet['geo']],
                                   'coordinates': [tweet['coordinates']],
                                   'place': [str(tweet['place'])],
                                   'contributors': [tweet['contributors']],
                                   'retweet_count': [tweet['retweet_count']],
                                   'favorite_count': [tweet['favorite_count']],
                                   'favorited': [tweet['favorited']],
                                   'retweeted': [tweet['retweeted']],
                                   'lang': [tweet['lang']],
                                   'type': tweet_type
                                   })

        # If not a regular tweet, extract entities from original tweet.
        if tweet_type == "rt":
            tweet = tweet["retweeted_status"]
        if tweet_type == "quote":
            tweet = tweet["quoted_status"]

        # Parse entities in tweet (mentions and hashtags)
        if 'entities' in tweet_keys:
            for mention in tweet['entities']['user_mentions']:
                tmp_mentions = pd.DataFrame({'tweet_id': [str(tweet_id)],
                                             'user_id': [str(mention['id'])]})
                l_mentions.append(tmp_mentions)

            for hashtag in tweet['entities']['hashtags']:
                tmp_hash = pd.DataFrame({'tweet_id': str(tweet_id),
                                         'hashtag': [hashtag['text']]})
                l_hashtags.append(tmp_hash)

        l_tweets.append(tmp_tweets)

    if len(l_tweets) > 0:
        df_tweets = pd.concat(l_tweets).reset_index(drop=True)
    else:
        df_tweets = None

    if len(l_mentions) > 0:
        df_mentions = pd.concat(l_mentions).reset_index(drop=True)
    else:
        df_mentions = None

    if len(l_hashtags) > 0:
        df_hashtags = pd.concat(l_hashtags).reset_index(drop=True)
    else:
        df_hashtags = None

    return df_tweets, df_mentions, df_hashtags


def create_users_dataframe(tweets):
    COLUMNS_USERS = ["id", "name", "screen_name", "location", "followers_count", "friends_count", "created_at",
                     "favourites_count",
                     "time_zone", "geo_enabled", "verified", "statuses_count", "lang"]
    users = {str(tweet["user"]["id"]): tweet["user"] for tweet in tweets}
    df_users = pd.DataFrame(users.values())[COLUMNS_USERS]
    df_users = df_users.rename(columns={"id": "user_id"})
    df_users["user_id"] = df_users["user_id"].astype(str)
    return df_users


def update_pickle(file, data, id_field):
    data["downloaded_at"] = datetime.datetime.now()
    try:
        data_old = pd.read_pickle(file, compression="gzip")
        data_old_ids = [str(i) for i in data_old[id_field]]
        data_new_ids = [str(i) for i in data[id_field]]
        ids_remove = [i for i in data_old_ids if i in data_new_ids]
        data_old = data_old[~data_old[id_field].isin(ids_remove)]
        data = pd.concat([data_old, data]).reset_index(drop=True)
    except OSError:
        pass
    data.to_pickle(file, compression="gzip")


def update_data_files(tweets):
    tweets_df, mentions_df, hashtags_df = create_tweets_mentions_hashtags_dataframes(tweets)
    users_df = create_users_dataframe(tweets)

    if tweets_df is not None:
        utils.log_and_print(f"- Saving tweets ({len(tweets_df)})")
        update_pickle(f"{OUTPUT_PATH}/{MONTH_PREFIX}_tweets.pkl", tweets_df, "tweet_id")

    if mentions_df is not None:
        utils.log_and_print(f"- Saving mentions ({len(mentions_df)})")
        update_pickle(f"{OUTPUT_PATH}/{MONTH_PREFIX}_mentions.pkl", mentions_df, "tweet_id")

    if hashtags_df is not None:
        utils.log_and_print(f"- Saving hashtags ({len(hashtags_df)})")
        update_pickle(f"{OUTPUT_PATH}/{MONTH_PREFIX}_hashtags.pkl", hashtags_df, "tweet_id")

    if users_df is not None:
        utils.log_and_print(f"- Saving users ({len(users_df)})")
        update_pickle(f"{OUTPUT_PATH}/{MONTH_PREFIX}_users.pkl", users_df, "user_id")

def retrieve_tweets_from_file(file, number_of_tweets=100):
    entities = [line.rstrip('\n') for line in open(file)]

    for entity in tqdm(entities):

        utils.log_and_print(entity)
        tweets = utils.retrieve_from_twitter(entity, number_of_tweets)

        # Only update when there is at least one tweet retrieved
        if len(tweets) > 0:
            update_data_files(tweets)

if __name__ == '__main__':
    # python src/retrieve_tweets.py retrieve_tweets_from_file --file='data/entities_to_retrieve.txt' --number_of_tweets=1000
    fire.Fire()

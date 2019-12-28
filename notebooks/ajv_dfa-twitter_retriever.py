from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv_dfa-first_draft')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

# +
from utils import utils

import pandas as pd
# -

ENTITIES_TO_RETRIEVE = Path(source_path).parent / 'data' / "entities_to_retrieve_sample.txt"


# +
def retrieve_tweets_from_file(file):
    entities = [line.rstrip('\n') for line in open(file)]
    for entity in entities:
        tweets = utils.retrieve_from_twitter(entity)
        update_data_files(tweets)

def update_data_files(tweets):
    FILE_TWEETS = Path(source_path).parent / 'data' / "tweets.csv"
    FILE_MENTIONS = Path(source_path).parent / 'data' / "mentions.csv"
    FILE_HASHTAGS = Path(source_path).parent / 'data' / "hashtags.csv"
    FILE_USERS = Path(source_path).parent / 'data' / "users.csv"
    tweets_df, mentions_df, hashtags_df = create_tweets_mentions_hashtags_dataframes(tweets)
    users_df = create_users_dataframe(tweets)
    update_data_file(FILE_TWEETS, tweets_df, "tweet_id")
    update_data_file(FILE_MENTIONS, mentions_df, "tweet_id")
    update_data_file(FILE_HASHTAGS, hashtags_df, "tweet_id")
    update_data_file(FILE_USERS, users_df, "user_id")


# +
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
    
    df_tweets = pd.concat(l_tweets).reset_index(drop=True)
    df_mentions = pd.concat(l_mentions).reset_index(drop=True)
    df_hashtags = pd.concat(l_hashtags).reset_index(drop=True)
    
    return df_tweets, df_mentions, df_hashtags

def create_users_dataframe(tweets):
    COLUMNS_USERS = ["id", "name", "screen_name", "location", "followers_count", "friends_count", "created_at", "favourites_count",
                     "time_zone", "geo_enabled", "verified", "statuses_count", "lang"]
    users = {tweet["user"]["id"]:tweet["user"] for tweet in tweets}
    df_users = pd.DataFrame(users.values())[COLUMNS_USERS]
    df_users = df_users.rename(columns={"id": "user_id"})
    return df_users

def update_data_file(file, data, id_field):
    SEPARATOR = ";"
    try:
        data_old = pd.read_csv(file, sep=SEPARATOR)
        data_old_ids = data_old[id_field]
        data_new_ids = [str(i) for i in data[id_field]]
        ids_remove = [i for i in data_old_ids if i in data_new_ids]
        data_old = data_old[~data_old[id_field].isin(ids_remove)]
        data = pd.concat([data_old, data])
    except FileNotFoundError:
        pass
    data.to_csv(file, index=False, encoding="utf-8", sep=SEPARATOR)


# -

retrieve_tweets_from_file(ENTITIES_TO_RETRIEVE)

"""
Useful, generic functions
"""

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent.parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

import datetime
import logging
import tweepy
import yaml


def connect_to_twitter_api(wait_on_rate_limit=False):
    twitter_keys = load_config()['default']['twitter']

    auth = tweepy.OAuthHandler(twitter_keys['consumer_key'],
                               twitter_keys['consumer_secret'])

    auth.set_access_token(twitter_keys['access_token_key'],
                          twitter_keys['access_token_secret'])

    return tweepy.API(auth, wait_on_rate_limit=wait_on_rate_limit)


def load_config():
    return open_yaml(Path(os.path.abspath(__file__)).parent.parent.parent / 'config.yaml')


def open_yaml(path):
    """
    Load yaml file

    Parameters
    ----------
    path: pathlib.PosixPath
        Path to yaml file
    Return
    ------
    Dictionary
        Dictionary with yaml file content
    """

    with open(path) as stream:
        try:
            yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error('Error when opening YAML file.', exc_info=1)

    return yaml_dict


def retrieve_from_twitter(entity, number_of_tweets=100, wait_on_rate_limit=True, max_id=None,
                          since_id=None):
    if '@' in entity:
        return retrieve_tweets_from_user(entity, number_of_tweets, wait_on_rate_limit, max_id, since_id)

    elif '#' in entity:
        return retrieve_tweets_from_hashtag(entity, number_of_tweets, wait_on_rate_limit, max_id, since_id)

    logging.error("Should pass a username or hashtag with the proper format (@, #)")


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


def retrieve_tweets_from_hashtag(hashtag, number_of_tweets=100, wait_on_rate_limit=False, max_id=None,
                                 since_id=None):
    api = connect_to_twitter_api(wait_on_rate_limit)

    tweets = []

    for tweet in tweepy.Cursor(api.search, q=hashtag,
                               max_id=max_id, since_id=since_id,
                               tweet_mode='extended').items(number_of_tweets):
        tweets.append(tweet._json)

    return tweets


def log_and_print(message):
    LOG_FILE = "data/log.txt"
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as file:
        file.write(f"{now} {message}")
        file.write("\n")
    print(message)
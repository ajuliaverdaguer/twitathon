"""
Useful, generic functions
"""

import os
import re
import sys

from pathlib import Path

source_path = str(Path(os.path.abspath(__file__)).parent.parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

import datetime
import logging
import pandas as pd
import tweepy
import yaml

from langdetect import detect, lang_detect_exception

def connect_to_twitter_api(wait_on_rate_limit=False):
    twitter_keys = load_config()['default']['twitter']

    auth = tweepy.OAuthHandler(twitter_keys['consumer_key'],
                               twitter_keys['consumer_secret'])

    auth.set_access_token(twitter_keys['access_token_key'],
                          twitter_keys['access_token_secret'])

    return tweepy.API(auth, wait_on_rate_limit=wait_on_rate_limit)


def detect_text_language(text):
    """
    Detect language from text using langdetect package

    Parameters
    ----------
    text: string
        Text from which to detect the language
    Return
    ------
    str
        Language detected
    """

    try:
        lang = detect(text)

    except lang_detect_exception.LangDetectException:
        lang = None

    return lang


def load_config():
    return open_yaml(Path(os.path.abspath(__file__)).parent.parent.parent / 'config' / 'config.yaml')


def load_paths():
    return open_yaml(Path(os.path.abspath(__file__)).parent.parent.parent / 'config' / 'paths.yaml')


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
    return []


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


def log_and_print(message, log_file):
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as file:
        file.write(f"{now} {message}")
        file.write("\n")
    print(message)


def read_data(file):
    df = pd.read_pickle(file, compression="gzip")
    return df


def read_entities_to_retrieve_file(file):
    return [line.rstrip('\n') for line in open(file)]


def remove_extra_spaces(text):
    text = re.sub(" +", " ", text)
    return text.strip()


def remove_newline_characters(text):
    return text.replace("\n", " ").replace("\r", "")

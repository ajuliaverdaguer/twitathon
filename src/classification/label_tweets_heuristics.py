"""
Label tweets according to heuristic rules
    These rules come from "Regles" spreadsheet in our Drive
"""

# TODO: Refactor code!

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent.parent)
data_path = Path(source_path).parent / 'data'
if source_path not in sys.path:
    sys.path.insert(0, source_path)

import fire
import pandas as pd

from utils.paths import PATH_ORIGINAL_HASHTAGS, PATH_ORIGINAL_USERS_IDS


def check_hashtags(tweet_text, a_priori_hashtag_classification=None):

    if a_priori_hashtag_classification is None:
        a_priori_hashtag_classification = pd.read_csv(data_path / 'original_hashtags.csv')

    available_hashtags = []
    for i, row in a_priori_hashtag_classification.iterrows():

        hashtag = row.hashtag.lower()
        category = row.category

        if hashtag in tweet_text:
            available_hashtags.append(category)

    unique_categories = set(available_hashtags)
    if len(unique_categories) == 1:
        return unique_categories[0]

    # If there is no hashtag a priori classified, or the tweet contains hashtags with opposite categories, return None
    return None


def check_user(user_id):

    data = pd.read_csv(PATH_ORIGINAL_USERS_IDS)

    if user_id in data['user_id']:

        return data[data['user_id'] == user_id, 'category']

    return None


def apply_antiracist_user_rule(user_id, a_priori_user_classification=None):

    if a_priori_user_classification is None:

        return 0 if check_user(user_id) == 'antiracist' else None

    if user_id in a_priori_user_classification['user_id']:

        if a_priori_user_classification[a_priori_user_classification['user_id'] == user_id, 'category'] == 'antiracist':

            return 0

    return None


def apply_racist_user_hashtag_rule(user_id, tweet, a_priori_user_classification=None,
                                   a_priori_hashtag_classification=None):

    if (a_priori_user_classification is None) & (a_priori_hashtag_classification is None):

        return 1 if (check_user(user_id) == 'racist') & (check_hashtags(tweet) == 'racist') else None

    if user_id in a_priori_user_classification['user_id']:

        if (a_priori_user_classification[a_priori_user_classification['user_id'] == user_id, 'category'] == 'racist') &\
                (check_hashtags(tweet, a_priori_hashtag_classification) == 'racist'):
            return 1

    return None


def label_tweets_from_heuristics(tweets, rules=None):

    user_classification = pd.read_csv(PATH_ORIGINAL_USERS_IDS)
    hashtag_classification = pd.read_csv(PATH_ORIGINAL_HASHTAGS)

    for rule in rules:

        print(f"Applying rule {rule}")

        column_name = rule.replace('apply_', '') if rule.startswith('apply_') else rule

        if rule == 'apply_antiracist_user_rule':
            tweets[column_name] = tweets['user_id'].apply(lambda x: apply_antiracist_user_rule(x, user_classification))

        if rule == 'apply_racist_user_hashtag_rule':

            tweets[column_name] = None

            for i, row in tweets.iterrows():

                tweets.loc[i, column_name] = apply_racist_user_hashtag_rule(row['user_id'], row['text'],
                                                                            user_classification,
                                                                            hashtag_classification)

    return tweets


if __name__ == '__main__':

    # python src/label_tweets_heuristics.py label_tweets_from_heuristics
    # fire.Fire()

    # For debugging
    tweets = pd.read_pickle(data_path / 'tweets.pkl', compression="gzip")[['tweet_id', 'user_id', 'text']][1:1000]
    labelled = label_tweets_from_heuristics(tweets, rules=['apply_antiracist_user_rule',
                                                           'apply_racist_user_hashtag_rule'])
    print('Done')

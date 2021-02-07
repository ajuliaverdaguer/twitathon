"""
Classify tweets from clean dataset with a priori rules
"""

import fire
import gzip
import pandas as pd
from pathlib import Path
import pickle5 as pickle

from classification.label_tweets_heuristics import label_tweets_from_heuristics
from utils.paths import PATH_FOLDER_DATASET, PATH_ORIGINAL_USERS_IDS
from utils.utils import read_data

# Settings
DATASET_VERSION = "01"

# Paths
CLEAN_DATASET = f"{PATH_FOLDER_DATASET}/dataset_v{DATASET_VERSION}.pkl"
OUTPUT_FILE = f"{PATH_FOLDER_DATASET}/classified_dataset_v{DATASET_VERSION}.pkl"


def read_data(file):  # TODO: Remove from here once requirements fixed!!!

    with gzip.open(file, "rb") as fh:
        data = pickle.load(fh)
    return data


def classify_clean_dataset():

    print("Classifying clean dataset!")

    print("- Reading clean dataset...")
    data = read_data(CLEAN_DATASET)
    data['user_id'] = data['user_id'].apply(int)

    print("- Reading a priori dataset and keeping tweets only from those users...")
    user_classification = pd.read_csv(PATH_ORIGINAL_USERS_IDS)
    data = data[data['user_id'].isin(user_classification['user_id'])]

    print("- Classifying tweets from a priori rules...")
    output = label_tweets_from_heuristics(data, rules=['apply_antiracist_user_rule', 'apply_racist_user_hashtag_rule'])
    output['is_hate'] = None
    output.loc[output["antiracist_user_rule"] == 0, 'is_hate'] = 0
    output.loc[output["racist_user_hashtag_rule"] == 1, 'is_hate'] = 1

    print("- Removing unnececessary columns...")
    columns_keep = ["tweet_id", "text", "antiracist_user_rule", "racist_user_hashtag_rule", "is_hate"]
    output = output[columns_keep]

    print("Saving classified dataset file...")
    output.to_pickle(OUTPUT_FILE, compression="gzip")


if __name__ == '__main__':

    # python src/classify_clean_dataset_heuristics.py classify_clean_dataset
    fire.Fire()

"""
Classify tweets from clean dataset with a priori rules
"""

import fire
import gzip
import pandas as pd
from pathlib import Path
import pickle5 as pickle

from classification.label_tweets_heuristics import label_tweets_from_heuristics
from utils.paths import PATH_FOLDER_DATASET
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

    print("- Classifying tweets from a priori rules...")
    data['user_id'] = 'abc'  # TODO: Remove once this column comes from create_dataset.py!
    output = label_tweets_from_heuristics(data, rules=['apply_antiracist_user_rule', 'apply_racist_user_hashtag_rule'])

    print("- Removing unnececessary columns...")
    columns_keep = ["tweet_id", "text", "antiracist_user_rule", "racist_user_hashtag_rule"]
    output = output[columns_keep]

    print("Saving classified dataset file...")
    output.to_pickle(OUTPUT_FILE, compression="gzip")


if __name__ == '__main__':

    # python src/classify_clean_dataset_heuristics.py classify_clean_dataset
    fire.Fire()

"""
Create dataset from raw files.
"""

import pandas as pd
from pathlib import Path
from utils.utils import read_data, detect_text_language, remove_extra_spaces, remove_newline_characters

# Settings
RAW_DATA_FOLDER = "data/raw"
DATASET_FOLDER = "data/dataset"
DATASET_VERSION = "01"
LANGUAGE = "es"

def find_files(folder, name):
    return list(Path(folder).rglob(name))

def merge_files(files):
    data = list()
    for file in files:
        data.append(read_data(file))
    data = pd.concat(data)
    id_field = data.columns[0]
    data = data.sort_values("downloaded_at")
    return data.drop_duplicates(subset=[id_field], keep="last").reset_index(drop=True)

def clean_text(text):
    text_clean = remove_newline_characters(text)
    return remove_extra_spaces(text_clean)

# Retrieve data
print("Retrieving data...")
files_tweets = find_files(RAW_DATA_FOLDER, "tweets.pkl")
tweets = merge_files(files_tweets)

# Create dataset
print("Creating dataset...")
dataset = tweets.copy()

# Filter-out retweets
print("- Filtering-out retweets...")
dataset = dataset[dataset["type"] != "rt"].reset_index(drop=True)

# Remove unnecessary columns
print("- Removing unncecessary columns...")
columns_keep = ["tweet_id", "text"]
dataset = dataset[columns_keep]

# Clean text
print("- Cleaning text...")
dataset["text"] = dataset["text"].apply(clean_text)

# Filter by language
print("- Filtering by language...")
dataset["language"] = dataset["text"].apply(detect_text_language)
dataset = dataset[dataset["language"] == LANGUAGE]
dataset = dataset.drop(columns="language").reset_index(drop=True)

# Save dataset file
print("Saving dataset file...")
output_file = f"{DATASET_FOLDER}/dataset_v{DATASET_VERSION}.pkl"
dataset.to_pickle(output_file, compression="gzip")
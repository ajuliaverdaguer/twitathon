"""
Create dataset from raw files.
"""

import pandas as pd
from pathlib import Path
from utils.utils import read_data, detect_text_language, remove_extra_spaces, remove_newline_characters

# Settings
DATASET_VERSION = "01"
LANGUAGE = "es"
THRESHOLD_ENTITY_WORDS = 0.8

# Paths
RAW_DATA_FOLDER = "data/raw"
DATASET_FOLDER = "data/dataset"
LANGUAGES_FILE = f"{DATASET_FOLDER}/tweets_language_cache.csv"
OUTPUT_FILE = f"{DATASET_FOLDER}/dataset_v{DATASET_VERSION}.pkl"


def find_files(folder, name):
    return list(Path(folder).rglob(f"*{name}"))


def merge_files(files):
    data = list()
    for file in files:
        data.append(read_data(file))
    data = pd.concat(data)
    id_field = data.columns[0]
    data = data.sort_values("downloaded_at")
    return data.drop_duplicates(subset=[id_field], keep="last").reset_index(drop=True)


def load_languages_cache(languages_cache_file):
    try:
        return pd.read_csv(languages_cache_file, dtype=str, encoding="utf-8")
    except OSError:
        return pd.DataFrame(columns=["tweet_id", "language"])


def save_languages_cache(languages_cache_file, dataset):
    dataset["language"][dataset["language"].isnull()] = "unknown"
    language_cache = load_languages_cache(languages_cache_file)
    new_languages = dataset[["tweet_id", "language"]]
    all_languages = pd.concat([language_cache, new_languages]).drop_duplicates("tweet_id", keep='last')
    all_languages.to_csv(languages_cache_file, index=False, encoding="utf-8")


def remove_urls(message):
    return " ".join([w for w in message.split(" ") if w[:4] != "http"])


def clean_text(message):
    text_clean = remove_newline_characters(message)
    text_clean = remove_extra_spaces(text_clean)
    text_clean = remove_urls(text_clean)
    return text_clean


def calculate_entity_word_ratio(message):
    words = message.split(" ")
    hashtags = [w for w in words if w[0] in ["#", "@"]]
    return len(hashtags) / len(words)


# Retrieve data
print("Retrieving data...")
files_tweets = find_files(RAW_DATA_FOLDER, "tweets.pkl")
print(f"- {len(files_tweets)} files found.")
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
dataset = dataset[dataset["text"] != ""].reset_index(drop=True)

# Remove messages with too much entities
print("- Removing messages with too much entities...")
dataset["entity_ratio"] = dataset["text"].apply(calculate_entity_word_ratio)
dataset = dataset[dataset["entity_ratio"] < THRESHOLD_ENTITY_WORDS].reset_index(drop=True)
dataset = dataset.drop(columns="entity_ratio")

# Filter by language
print("- Filtering by language...")
languages_cache = load_languages_cache(LANGUAGES_FILE)
dataset = dataset.merge(languages_cache, on="tweet_id", how="left").fillna("")
missing_languages = dataset["language"] == ""
print(f"  Estimating language of {missing_languages.sum()} tweets...")
dataset["language"][missing_languages] = dataset["text"][missing_languages].apply(detect_text_language)
save_languages_cache(LANGUAGES_FILE, dataset)
dataset = dataset[dataset["language"] == LANGUAGE]
dataset = dataset.drop(columns="language").reset_index(drop=True)

# Save dataset file
print("Saving dataset file...")
dataset.to_pickle(OUTPUT_FILE, compression="gzip")

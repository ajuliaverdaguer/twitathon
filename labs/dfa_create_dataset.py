# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.5.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# cd ..

# +
import pandas as pd

from pathlib import Path

from src.utils.utils import read_data, detect_text_language, remove_extra_spaces, remove_newline_characters
# -

# # Settings

DATA_FOLDER = "data/datasets"
LANGUAGE = "es"
VERSION = "01"


# # Functions

# +
def find_files(name):
    return list(Path(DATA_FOLDER).rglob(name))

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


# -

# # Retrieve data

# ## Tweets

files_tweets = find_files("tweets.pkl")
tweets = merge_files(files_tweets)

# ## Users

files_users = find_files("users.pkl")
users = merge_files(files_users)

# # Create dataset

# +
dataset = tweets.copy()

#Sample
#dataset = dataset.head(1000)
# -

# ## Filter-out retweets

dataset = dataset[dataset["type"] != "rt"].reset_index(drop=True)

# ## Add user name

dataset = dataset.merge(users[["user_id", "screen_name"]], on="user_id", how="left")
dataset = dataset.rename(columns={"screen_name": "user"})

# ## Remove unnecessary columns

columns_keep = ["tweet_id", "text", "user"]
dataset = dataset[columns_keep]

# ## Clean text

dataset["text"] = dataset["text"].apply(clean_text)

# More Suggested actions TODO:
# - Remove hashtags?
# - Remove users?
# - Replace users by real name?
# - Remove empty or too short messages

# ## Filter by language

dataset["language"] = dataset["text"].apply(detect_text_language)
dataset = dataset[dataset["language"] == LANGUAGE]
dataset = dataset.drop(columns="language").reset_index(drop=True)

# # Save dataset file

output_file = f"{DATA_FOLDER}/dataset_v{VERSION}.pkl"
dataset.to_pickle(output_file, compression="gzip")

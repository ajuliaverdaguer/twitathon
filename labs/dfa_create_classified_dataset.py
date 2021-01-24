# -*- coding: utf-8 -*-
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
import numpy as np
import pandas as pd

from pathlib import Path

from src.utils.utils import read_data
# -

# # Settings

DATASET_FILE = "data/dataset/dataset_v01.pkl"
RAW_DATA_FOLDER = "data/raw"
USERS_CATEGORY_FILE = "data/entities/original_users.csv"
HASHTAG_CATEGORY_FILE = "data/entities/original_hashtags.csv"
OUTPUT_FILE = "data/dataset/dataset_label.pkl"


# # Functions

# +
def find_files(folder, name):
    return list(Path(folder).rglob(f"*{name}"))

def merge_files(files, id_field):
    data = list()
    for file in files:
        data.append(read_data(file))
    data = pd.concat(data)
    data = data.sort_values("downloaded_at")
    return data.drop_duplicates(subset=[id_field], keep="last").reset_index(drop=True)

def contains_any_hashtag(text, hashtag_list):
    text = text.lower()
    for hashtag in hashtag_list:
        hashtag = hashtag.lower()
        if hashtag in text:
            return True
    return False


# -

# # Retrieve data

# ## Dataset

dataset = read_data(DATASET_FILE)
dataset.head(2)

# ## Users

files_users = find_files(RAW_DATA_FOLDER, "users.pkl")
users = merge_files(files_users, id_field="user_id")
users = users[["user_id", "screen_name"]]
users.head(2)

# ## Hashtags

# + active=""
# files_hashtags = find_files(RAW_DATA_FOLDER, "hashtags.pkl")
# hashtags = merge_files(files_hashtags, id_field="tweet_id")
# hashtags = hashtags[["tweet_id", "hashtag"]]
# hashtags.head(2)
# -

# ## Users category

users_category = pd.read_csv(USERS_CATEGORY_FILE, usecols=["username", "category"])
users_category.head(2)

# ## Hashtag category

hashtags_category = pd.read_csv(HASHTAG_CATEGORY_FILE, usecols=["hashtag", "category"])
hashtags_category.head(2)

# # Rules
# https://docs.google.com/spreadsheets/d/1gg0IWYzKyVcuuaVb-HfDHWLyKMaFzWikys0qiOIAiP0/edit?usp=sharing

# ## Rule 1
# Regla 1: si és un tweet d'un usuari definit com a "No Racista" o "Antirracista", llavors el tweet NO és Racista

users_r1 = users.copy()
users_r1["screen_name_low"] = users_r1["screen_name"].apply(lambda x: x.lower())
users_r1.head(2)

users_category_r1 = users_category.copy()
users_category_r1["screen_name_low"] = users_category_r1["username"].apply(lambda x: x[1:].lower())
users_category_r1.head(2)

users_antiracist = users_category_r1.merge(users_r1, on="screen_name_low", how="left")
users_antiracist = users_antiracist[users_antiracist["category"] == "antiracist"]
users_antiracist["rule1"] = False
users_antiracist = users_antiracist[["user_id", "rule1"]]
users_antiracist.head(2)

dataset_r1 = dataset.merge(users_antiracist, on="user_id", how="left")
dataset_r1 = dataset_r1[["tweet_id", "rule1"]]
dataset_r1.head(2)

# ## Rule 2
# Regla 2: si és un tweet d'un usuari definit com a "Racista" i a més el tweet Inclou un hashtag racista, llavors el tweet és Racista

users_r2 = users.copy()
users_r2["screen_name_low"] = users_r2["screen_name"].apply(lambda x: x.lower())
users_r2.head(2)

users_category_r2 = users_category.copy()
users_category_r2["screen_name_low"] = users_category_r2["username"].apply(lambda x: x[1:].lower())
users_category_r2.head(2)

users_racist = users_category_r2.merge(users_r2, on="screen_name_low", how="left")
users_racist = users_racist[users_racist["category"] == "racist"]
users_racist["rule2_user"] = True
users_racist = users_racist[["user_id", "rule2_user"]]
users_racist.head(2)

# + active=""
# hashtags_r2 = hashtags.copy()
# hashtags_r2["hashtag_low"] = hashtags_r2["hashtag"].apply(lambda x: x.lower())
# hashtags_r2.head(2)
# -

hashtags_category_r2 = hashtags_category.copy()
hashtags_category_r2["hashtag_low"] = hashtags_category_r2["hashtag"].apply(lambda x: x[1:].lower())
hashtags_category_r2.head(2)

hashtags_racist_list = list(hashtags_category_r2[hashtags_category_r2["category"] == "racist"]["hashtag"])
hashtags_racist_list

dataset_r2 = dataset.merge(users_racist, on="user_id", how="left").fillna(False)
dataset_r2.head(2)

dataset_r2["rule2_hashtag"] = dataset_r2["text"].apply(lambda x: contains_any_hashtag(x, hashtags_racist_list))
dataset_r2.head(2)

dataset_r2["rule2"] = dataset_r2.apply(lambda row: row["rule2_user"] and row["rule2_hashtag"], axis=1)
dataset_r2 = dataset_r2[["tweet_id", "rule2"]]
dataset_r2.head(2)

dataset_r2 = dataset.merge(dataset_r2[dataset_r2["rule2"]], on="tweet_id", how="left")
dataset_r2 = dataset_r2[["tweet_id", "rule2"]]
dataset_r2.head(2)

# # Final label

dataset_label = dataset.merge(dataset_r1, on="tweet_id", how="left")
dataset_label = dataset_label.merge(dataset_r2, on="tweet_id", how="left")
dataset_label.head(2)

false_positions = list(dataset_label["rule1"] == False)
true_positions = list(dataset_label["rule2"] == True)
dataset_label["is_hate"] = np.nan
dataset_label["is_hate"][false_positions] = False
dataset_label["is_hate"][true_positions] = True
dataset_label.head(2)

# # Save file

dataset_label.to_pickle(OUTPUT_FILE, compression="gzip")

# # Analysis

dataset_hate = dataset_label[dataset_label["is_hate"] == True]
print(len(dataset_hate))
dataset_hate = dataset_hate.merge(users[["user_id", "screen_name"]], on="user_id", how="left")
dataset_hate.head(5)

for t in dataset_hate["text"]:
    print(t)
    print("")

dataset_no_hate = dataset_label[dataset_label["is_hate"] == False]
print(len(dataset_no_hate))
dataset_no_hate = dataset_no_hate.merge(users[["user_id", "screen_name"]], on="user_id", how="left")
dataset_no_hate.head(5)

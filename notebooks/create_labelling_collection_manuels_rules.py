# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# cd ..

# %load_ext autoreload
# %autoreload 2

# +
import numpy as np
import pandas as pd

from omegaconf import OmegaConf

from src.utils.utils import read_data, get_all_db_files, get_complete_table
# -

paths = OmegaConf.load("config/paths.yaml")

# # Settings

DATASET = "dataset_v04"
SIZE = 400
COLLECTION_NUM = "08"

SEED = int(COLLECTION_NUM)

# ## Paths

DATASET_PATH = f"data/datasets/{DATASET}.pkl"
USERS_CATEGORY_FILE = datasets_folder = paths.default.data.original_users
HASHTAG_CATEGORY_FILE = datasets_folder = paths.default.data.original_hashtags
#CLASSIFIED_DATASET_PATH = "data/datasets/dataset_label.pkl"
OUTPUT_PATH = f"data/labelling/collections/collection_{COLLECTION_NUM}.csv"

# # Retrieve dataset

dataset = read_data(DATASET_PATH)
dataset.head(5)

# # Assign pre-label

# ## Retrieve extra data

files_db = get_all_db_files()

# ### Users

users = get_complete_table(files_db, table="users", id_field="user_id")
users = users[["user_id", "screen_name"]]
users.head(2)

# ### Users category

users_category = pd.read_csv(USERS_CATEGORY_FILE, usecols=["username", "category"])
users_category.head(2)

# ### Hashtags category

hashtags_category = pd.read_csv(HASHTAG_CATEGORY_FILE, usecols=["hashtag", "category"])
hashtags_category.head(2)

# ### Rules

# https://docs.google.com/spreadsheets/d/1gg0IWYzKyVcuuaVb-HfDHWLyKMaFzWikys0qiOIAiP0/edit?usp=sharing

# #### Rule 1

users_r1 = users.copy()
users_r1["screen_name_low"] = users_r1["screen_name"].apply(lambda x: x.lower())

users_category_r1 = users_category.copy()
users_category_r1["screen_name_low"] = users_category_r1["username"].apply(lambda x: x[1:].lower())

users_antiracist = users_category_r1.merge(users_r1, on="screen_name_low", how="left")
users_antiracist = users_antiracist[users_antiracist["category"] == "antiracist"]
users_antiracist["rule1"] = False
users_antiracist = users_antiracist[["user_id", "rule1"]]

dataset_r1 = dataset.merge(users_antiracist, on="user_id", how="left")
dataset_r1 = dataset_r1[["tweet_id", "rule1"]]

# #### Rule 2

users_r2 = users.copy()
users_r2["screen_name_low"] = users_r2["screen_name"].apply(lambda x: x.lower())

users_category_r2 = users_category.copy()
users_category_r2["screen_name_low"] = users_category_r2["username"].apply(lambda x: x[1:].lower())

users_racist = users_category_r2.merge(users_r2, on="screen_name_low", how="left")
users_racist = users_racist[users_racist["category"] == "racist"]
users_racist["rule2_user"] = True
users_racist = users_racist[["user_id", "rule2_user"]]

hashtags_category_r2 = hashtags_category.copy()
hashtags_category_r2["hashtag_low"] = hashtags_category_r2["hashtag"].apply(lambda x: x[1:].lower())

hashtags_racist_list = list(hashtags_category_r2[hashtags_category_r2["category"] == "racist"]["hashtag"])

dataset_r2 = dataset.merge(users_racist, on="user_id", how="left").fillna(False)


# +
def contains_any_hashtag(text, hashtag_list):
    text = text.lower()
    for hashtag in hashtag_list:
        hashtag = hashtag.lower()
        if hashtag in text:
            return True
    return False

dataset_r2["rule2_hashtag"] = dataset_r2["text"].apply(lambda x: contains_any_hashtag(x, hashtags_racist_list))
# -

dataset_r2["rule2"] = dataset_r2.apply(lambda row: row["rule2_user"] and row["rule2_hashtag"], axis=1)
dataset_r2 = dataset_r2[["tweet_id", "rule2"]]

dataset_r2 = dataset.merge(dataset_r2[dataset_r2["rule2"]], on="tweet_id", how="left")
dataset_r2 = dataset_r2[["tweet_id", "rule2"]]

# ### Final label

dataset_label = dataset.merge(dataset_r1, on="tweet_id", how="left")
dataset_label = dataset_label.merge(dataset_r2, on="tweet_id", how="left")

false_positions = list(dataset_label["rule1"] == False)
true_positions = list(dataset_label["rule2"] == True)
dataset_label["is_hate"] = np.nan
dataset_label["is_hate"][false_positions] = False
dataset_label["is_hate"][true_positions] = True
dataset_label.head(2)

dataset_classified = dataset_label[["tweet_id", "is_hate"]].copy()
dataset_classified = dataset_classified.fillna("u")
dataset_classified = dataset_classified.replace([True, False], ["y", "n"])
dataset_classified.head(5)

dataset = dataset.merge(dataset_classified, on="tweet_id", how="left")


# # Generate labelling collection

def generate_collection(dataset, strategy, balanced):
    if strategy == "random":
        subset_yes = dataset[dataset["is_hate"] == "y"].sample(n=int(SIZE/2), random_state=SEED)
        subset_no = dataset[dataset["is_hate"] == "n"].sample(n=int(SIZE/2), random_state=SEED)
        collection = pd.concat([subset_yes, subset_no]).sample(frac=1)
    return collection


collection = generate_collection(dataset, strategy="random", balanced=True)


# # Save collection

def save_collection(collection, file):
    collection["language"] = "es"
    collection["comment"] = ""
    collection = collection[["language", "is_hate", "text", "tweet_id", "comment"]]
    collection.to_csv(OUTPUT_PATH, index=False, sep="|", encoding="utf-8")


save_collection(collection, OUTPUT_PATH)

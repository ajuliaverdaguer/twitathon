# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# cd ..

# %load_ext autoreload
# %autoreload 2

# +
import pandas as pd
import re
import unidecode

from nltk.corpus import stopwords

from src.utils.utils import get_all_db_files, get_complete_table
# -
# # Settings

DATASET_FILE = "data/datasets/dataset_v07.pkl"
RAW_FILES_PATH = "data/raw/"
USERS_FILE = "data/entities/original_users_extreme.csv"
WORDS_RACES_FILE = "data/entities/words_races.txt"


# # Functions

def clean_message(message):
    stop_words = set(stopwords.words("spanish"))
    message = message.lower()
    message = unidecode.unidecode(message)
    message = re.sub("[^a-zA-Z]+", " ", message)
    message = message.strip()
    message = re.sub(' +', ' ', message)
    message = " ".join([word for word in message.split(" ") if word not in stop_words])
    return message


def contains_word(message, word_list):
    message_clean = clean_message(message)
    words = message_clean.split(" ")
    return len(set(words) & set(word_list)) > 0


# # Data

# ## Retrieve data

# ### Dataset

dataset = pd.read_pickle(DATASET_FILE, compression="gzip")
dataset.head()

# ### Database files

files_db = get_all_db_files()

# #### Tweets

tweets = get_complete_table(files_db, table="tweets", id_field="tweet_id")
tweets.head()

# #### Users

users = get_complete_table(files_db, table="users", id_field="user_id")
users.head()

# ### User classification

users_type = pd.read_csv(USERS_FILE)
users_type.head()

# ### Words races

file = open(WORDS_RACES_FILE, "r")
words_races = [line[:-1] for line in file.readlines()]
words_races_clean = [clean_message(word) for word in words_races]
words_races_clean

# # Assignation

data = tweets[["tweet_id", "user_id"]].copy()

data = data.merge(users[["user_id", "screen_name"]], on="user_id", how="left")

users_type["screen_name"] = users_type["username"].apply(lambda x: x[1:])

data = data.merge(users_type[["screen_name", "category"]], on="screen_name", how="left")

data = data.dropna()

data = data[["tweet_id", "category"]]

data = data.rename(columns={"category": "category_user"})

data = dataset.merge(data, on="tweet_id", how="left")

data = data.dropna()

data["contains_race_word"] = data["text"].apply(lambda x: contains_word(x, words_races_clean))

data.head()

data["label"] = ""
data["label"][data["category_user"] == "antiracist"] = "n"
data["label"][(data["category_user"] == "racist") & (data["contains_race_word"] == True)] = "y"

data = data[data["label"] != ""]

data["label"].value_counts()

# ## Racist messages

for message in data[data["label"] == "y"]["text"]:
    print(message)
    print("")

# ## Non racist messages

for message in data[data["label"] == "n"]["text"]:
    print(message)
    print("")

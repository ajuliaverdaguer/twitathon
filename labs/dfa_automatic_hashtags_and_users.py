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
import datetime
import pandas as pd

from src.utils import utils
# -

# # Settings

HASHTAGS_FILE = "data/hashtags.pkl"
USERS_FILE = "data/users.pkl"
TWEETS_FILE = "data/tweets.pkl"
ENTITIES_TO_RETRIEVE_FILE = "data/entities_to_retrieve.txt"
ENTITIES_AUTOMATIC_FILE = "data/entities_automatic.pkl"
LOG_FILE = "data/log.txt"

TODAY = datetime.date.today()


# # Functions

# +
def read_entities_automatic(file):
    try:
        df = utils.read_data(ENTITIES_AUTOMATIC_FILE)
    except FileNotFoundError:
        df = pd.DataFrame(columns = ["entity_name", "entity_type", "date_top", "added_at"])
    return df
    
def get_top_entities_today(df, entity_name, number_of_entities=10):
    df[df["downloaded_at"].dt.date == TODAY]
    top_entities = list(df[entity_name].value_counts().index[:number_of_entities])
    top_entities_df = pd.DataFrame({"entity_name": top_entities})
    top_entities_df["entity_type"] = entity_name
    top_entities_df["date_top"] = TODAY
    top_entities_df["added_at"] = datetime.datetime.now()
    return top_entities_df

def log_changes(top_hashtags_df, top_users_df):
    if len(top_hashtags_df) > 0:
        utils.log_and_print(f"Adding new hashtags to retrieve ({len(top_hashtags_df)})")
    if len(top_users_df) > 0:
        utils.log_and_print(f"Adding new users to retrieve ({len(top_users_df)})")


# -

# # Retrieve data

# ## Hashtags

hashtags_df = utils.read_data(HASHTAGS_FILE)
hashtags_df = hashtags_df[["hashtag", "downloaded_at"]]
hashtags_df.head()

# ## Users

users_df = utils.read_data(USERS_FILE)
users_df = users_df[["user_id", "screen_name"]]
users_df = users_df.rename(columns = {"screen_name" : "user"})
users_df.head()

# ## Tweets

tweets_df = utils.read_data(TWEETS_FILE)
tweets_df = tweets_df[["user_id", "downloaded_at"]]
tweets_df.head()

# ## Entities to retrieve

entities_to_retrieve = utils.read_entities_to_retrieve_file(ENTITIES_TO_RETRIEVE_FILE)

# ## Entities automatic

entities_automatic_df = read_entities_automatic(ENTITIES_AUTOMATIC_FILE)

# # Get top entites

top_hashtags_df = get_top_entities_today(hashtags_df, "hashtag")
top_hashtags_df

users_df = tweets_df.merge(users_df, on="user_id", how="left").drop(columns=["user_id"])
top_users_df = get_top_entities_today(users_df, "user")
top_users_df

# # Filter entities in entities to retrieve

#  ## Hashtags

entities_to_retrieve_hashtags = [entity[1:].lower() for entity in entities_to_retrieve if entity[0] == "#"]
top_hashtags_df = top_hashtags_df[~top_hashtags_df["entity_name"].str.lower().isin(entities_to_retrieve_hashtags)]

# ## Users

entities_to_retrieve_users = [entity[1:].lower() for entity in entities_to_retrieve if entity[0] == "@"]
top_users_df = top_users_df[~top_users_df["entity_name"].str.lower().isin(entities_to_retrieve_users)]

# # Filter entities already in automatic entities

# ## Hashtags

hashtags_automatic_df = entities_automatic_df[entities_automatic_df["entity_type"] == "hashtag"]
top_hashtags_df = top_hashtags_df[~top_hashtags_df["entity_name"].str.lower().isin(hashtags_automatic_df["entity_name"].str.lower())]

# ## Users

users_automatic_df = entities_automatic_df[entities_automatic_df["entity_type"] == "user"]
top_users_df = top_users_df[~top_hashtags_df["entity_name"].str.lower().isin(hashtags_automatic_df["entity_name"].str.lower())]

# # Save file

entities_automatic_df_updated = pd.concat([entities_automatic_df, top_hashtags_df, top_users_df]).reset_index(drop=True)

log_changes(top_hashtags_df, top_users_df)
entities_automatic_df_updated.to_pickle(ENTITIES_AUTOMATIC_FILE, compression="gzip")

utils.read_data(ENTITIES_AUTOMATIC_FILE)

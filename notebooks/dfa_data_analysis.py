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

import pandas as pd

# # Settings

TWEETS_FILE = "../data/tweets.pkl"
HASHTAGS_FILE = "../data/hashtags.pkl"
MENTIONS_FILE = "../data/mentions.pkl"
USERS_FILE = "../data/users.pkl"


# # Functions

def read_data(file):
    df = pd.read_pickle(file, compression="gzip")
    return df


# # Retrieve data

tweets = read_data(TWEETS_FILE)

tweets = read_data(TWEETS_FILE)
print(len(tweets))
tweets.head()

hashtags = read_data(HASHTAGS_FILE)
print(len(hashtags))
hashtags.head()

hashtags

mentions = read_data(MENTIONS_FILE)
print(len(mentions))
mentions.head()

users = read_data(USERS_FILE)
print(len(users))
users.head()

# # Analysis

# ## Tweets

# ### Total tweets

print(len(tweets))

# ### Unique tweets

print(len(tweets["tweet_id"].unique()))

# ## Hashtags

# ### Total hashtags

print(len(hashtags))

# ### Unique hashtags

print(len(hashtags["hashtag"].unique()))

# ### Top hashtags

hashtags_summary = hashtags.groupby("hashtag").size().sort_values(ascending=False)
hashtags_summary.head(20)

# ## Users

# ### Total users

print(len(users))

# ### Unique users

print(len(users["user_id"].unique()))

# ### Top users

users_summary = tweets[["user_id"]].merge(users, on="user_id", how="left")
users_summary = users_summary.groupby(["user_id", "name"]).size().sort_values(ascending=False)
users_summary.head(20)

# ## Time evolution

dates = pd.to_datetime(tweets["created_at"], errors="coerce").dt.date
dates_summary = dates.value_counts()
dates_summary.sort_index().tail(20)

dates_summary.plot()

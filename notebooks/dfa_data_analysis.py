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

TWEETS_FILE = "../data/tweets.csv"
HASHTAGS_FILE = "../data/hashtags.csv"
MENTIONS_FILE = "../data/mentions.csv"
USERS_FILE = "../data/users.csv"


# # Functions

def read_data(file):
    df = pd.read_csv(file, encoding="utf-8", sep=";")
    return df


# # Retrieve data

tweets = read_data(TWEETS_FILE)

tweets = pd.read_csv(TWEETS_FILE, encoding="utf-8", sep=";", quoting=3, error_bad_lines=False)
print(len(tweets))
tweets.head(20)

hashtags = read_data(HASHTAGS_FILE)
print(len(hashtags))
hashtags.head()

mentions = read_data(MENTIONS_FILE)
print(len(mentions))
mentions.head()

users = read_data(USERS_FILE)
print(len(users))
users.head()

# # Counts

# ## Unique tweets

print(len(tweets))

print(len(tweets["tweet_id"].unique()))

# ## Unique hashtags

hashtags_summary = hashtags.groupby("hashtag").size().sort_values(ascending=False)

print(hashtags_summary.head(20))

print(len(hashtags_summary))

# ## Unique users

users_summary = users.groupby("name").size().sort_values(ascending=False)

print(users_summary.head(20))

print(len(users_summary))

# ## Time evolution

dates = pd.to_datetime(tweets["created_at"], errors="coerce").dt.date
dates_summary = dates.value_counts()
dates_summary

dates_summary.plot()

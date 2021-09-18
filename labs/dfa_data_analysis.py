# -*- coding: utf-8 -*-
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
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# cd ../..

# %load_ext autoreload
# %autoreload 2

# +
import pandas as pd

from glob import glob
from omegaconf import OmegaConf

from src.utils.sql import create_connection, read_table
# -

paths = OmegaConf.load("config/paths.yaml")

# # Retrieve data

raw_files_folder = paths.default.data.folder_raw
raw_files = glob(f"{raw_files_folder}/*.db")

for db_file in raw_files[:1]:
    print(db_file)
    tweets = read_table(db_file, "tweets")



# # Retrieve data

tweets = read_data(TWEETS_FILE)

tweets = read_data(TWEETS_FILE)
print(len(tweets))
tweets.head()

hashtags = read_data(HASHTAGS_FILE)
print(len(hashtags))
hashtags.head()

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

# ### Tweets per type

tweets.groupby("type").size()

# ### Tweets per language

tweets_lang = tweets.copy()
tweets_lang["lang"][~tweets_lang["lang"].isin(["en", "es", "ca"])] = "other"
tweets_lang.groupby("lang").size().sort_values(ascending=False)

# #### Tweets in Spanish

tweets_lang[tweets_lang["lang"] == "es"].groupby("type").size()

# + jupyter={"outputs_hidden": true} active=""
# for i in tweets_lang[(tweets_lang["lang"] == "es") & (tweets_lang["type"] == "regular")]["text"].tolist()[0:100]:
#     print(i)
# -

# ## Hashtags

# ### Total hashtags

print(len(hashtags))

# ### Unique hashtags

print(len(hashtags["hashtag"].unique()))

# ### Top hashtags

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

dates = pd.to_datetime(tweets["downloaded_at"], errors="coerce").dt.date
dates_summary = dates.value_counts()
dates_summary.sort_index().tail(20)

dates_summary.plot()



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

from src.utils.utils import read_data
# -

# # Settings

PREFIX = "2020-12"


# # Functions

# +
def print_dates_file(file):
    data = read_data(file)
    print(f"\tFrom {data['downloaded_at'].min()}")
    print(f"\t  To {data['downloaded_at'].max()}")
    print(f"\tSize {len(data)}")
    
def print_report(prefix):
    data_folder = "data/raw"
    if prefix == "":
        separator = ""
    else:
        separator = "_"
    common_path = f"{data_folder}/{prefix}{separator}"
    
    # Tweets
    tweets_file = f"{common_path}tweets.pkl"
    print(f"Tweets:")
    print_dates_file(tweets_file)
    
    # Users
    users_file = f"{common_path}users.pkl"
    print(f"Users:")
    print_dates_file(users_file)
    
    # Hashtags
    hashtags_file = f"{common_path}hashtags.pkl"
    print(f"Hashtags:")
    print_dates_file(hashtags_file)
    
    # Mentions
    mentions_file = f"{common_path}mentions.pkl"
    print(f"Mentions:")
    print_dates_file(mentions_file)


# -

# # Report

print_report(PREFIX)

# !pip3 install pickle5
import pickle5 as pickle
with open(path_to_protocol5, "rb") as fh:
  data = pickle.load(fh)

import pickle5 as pickle




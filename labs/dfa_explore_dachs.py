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
import math
import pandas as pd
import tweepy

from tqdm import tqdm

from src.utils.utils import connect_to_twitter_api
# -

# # Settings

TRAIN_HATE_FILE = "data/dachs/Train_Hate.csv"
OUTPUT_FILE = "data/dachs/Train_Hate_Messages.csv"


# # Functions

def retrieve_messages_from_id_list(id_list):
    api = connect_to_twitter_api(wait_on_rate_limit=True)
    tweets = api.statuses_lookup(id_list)
    return [(str(tweet._json["id"]), tweet._json["text"]) for tweet in tweets]


# # Retrieve data

hate = pd.read_csv(TRAIN_HATE_FILE, dtype={"tweetId": str, "Hate_Speech": int})
hate.head()

# ## List of ids

id_list = list(hate["tweetId"])

# # Retrieve text

chunk_size = 100
chunk_num = math.ceil(len(id_list) / chunk_size)
messages = list()
for i in tqdm(range(chunk_num)):
    start = chunk_size * i
    end = start + chunk_size
    chunk = id_list[start:end]
    messages = messages + retrieve_messages_from_id_list(chunk)

# # Save file

messages_df = pd.DataFrame({"tweetId": [t[0] for t in messages],
                            "message": [t[1] for t in messages]})

hate_full = hate.merge(messages_df, on="tweetId", how="left")
hate_full.head()

# # Save output

hate_full.to_csv(OUTPUT_FILE, index=False, sep="|", encoding="utf-8")

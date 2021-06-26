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

from src.utils.sql import read_table
# -

paths = OmegaConf.load("config/paths.yaml")

raw_files_folder = paths.default.data.folder_raw
raw_files = glob(f"{raw_files_folder}/*.db")

report = list()
for db_file in raw_files:
    tweets = read_table(db_file, "tweets")
    users = read_table(db_file, "users")
    mentions = read_table(db_file, "mentions")
    interval = db_file.split("\\")[-1][:-3]
    num_tweets = len(tweets)
    num_users = len(users)
    num_mentions = len(mentions)
    start_date = tweets['downloaded_at'].min()
    end_date = tweets['downloaded_at'].max()
    df = pd.DataFrame({"interval": [interval],
                       "from": [start_date[:10]],
                       "to": [end_date[:10]],
                       "tweets": [num_tweets],
                       "users": [num_users],
                       "mentions": [num_mentions]})
    report.append(df)
report = pd.concat(report).reset_index(drop=True)

report

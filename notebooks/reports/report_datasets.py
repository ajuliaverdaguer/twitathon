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
# -

paths = OmegaConf.load("config/paths.yaml")

datasets_folder = paths.default.data.folder_datasets
datasets_files = glob(f"{datasets_folder}/dataset_v*.pkl")

datasets_report = list()
for file in datasets_files:
    dataset = pd.read_pickle(file, compression="gzip")
    dataset_name = file.split("\\")[-1][:-4]
    num_tweets = len(dataset)
    num_tweets_unique = dataset["tweet_id"].nunique()
    assert num_tweets == num_tweets_unique, f"There are duplicated tweets in {dataset_name}."
    df = pd.DataFrame({"dataset": [dataset_name],
                       "num_tweets": [num_tweets]})
    datasets_report.append(df)
datasets_report = pd.concat(datasets_report).reset_index(drop=True)

datasets_report

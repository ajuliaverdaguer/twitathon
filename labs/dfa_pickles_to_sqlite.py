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

# cd ..

# %load_ext autoreload
# %autoreload 2

# +
import pandas as pd

from glob import glob
from omegaconf import OmegaConf

from src.utils.sql import create_connection
# -

paths = OmegaConf.load("config/paths.yaml")

raw_files_folder = paths.default.data.folder_raw
raw_files = glob(f"{raw_files_folder}/*_tweets.pkl")

for file in raw_files:
    prefix = file[len(raw_files_folder):-4-len("_tweets")]
    database_name = f"{raw_files_folder}/{prefix}.db"
    connection = create_connection(database_name)
    tables = ["tweets", "mentions", "users"]
    for table in tables:
        df = pd.read_pickle(f"{raw_files_folder}/{prefix}_{table}.pkl", compression="gzip")
        df = df.astype(str)
        with connection:
            df.to_sql(table, connection, index=False, if_exists="replace")

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

NUM = 2000
DATASET_VERSION = "01"
LABELLER = "manuel"
SEED = 31

# ## Paths

DATASET_PATH = f"data/dataset/dataset_v{DATASET_VERSION}.pkl"
OUTPUT_PATH = f"data/labels/{LABELLER}.csv"

# # Retrieve dataset

dataset = read_data(DATASET_PATH)

# # Generate labelling chunk

chunk = dataset.sample(n=NUM, random_state=SEED)

chunk["language"] = "es"
chunk["label"] = ""
chunk["trash"] = ""
chunk["comment"] = ""


# # Save chunk

def save_chunk(chunk, file):
    chunk = chunk[["language", "label", "trash", "text", "tweet_id", "comment"]]
    chunk.to_csv(OUTPUT_PATH, index=False, sep = "|", encoding="utf-8")


save_chunk(chunk, OUTPUT_PATH)

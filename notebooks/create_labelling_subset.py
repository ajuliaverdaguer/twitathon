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
LABELLER = "didac.fortuny"
SUBSET_NUM = "01"

SEED = 31

# ## Paths

DATASET_PATH = f"data/dataset/dataset_v{DATASET_VERSION}.pkl"
OUTPUT_PATH = f"data/labels/before_labeling/{LABELLER}/{LABELLER}_{SUBSET_NUM}.csv"

# # Retrieve dataset

dataset = read_data(DATASET_PATH)

import random
dataset["is_hate"] = [random.choice(["y", "n"]) for i in range(len(dataset))]


# # Generate labelling set

def generate_subset(dataset, strategy, balanced):
    if strategy == "random":
        subset_yes = dataset[dataset["is_hate"] == "y"].sample(n=int(NUM/2), random_state=SEED)
        subset_no = dataset[dataset["is_hate"] == "n"].sample(n=int(NUM/2), random_state=SEED)
        subset = pd.concat([subset_yes, subset_no]).sample(frac=1)
    return subset


subset = generate_subset(dataset, strategy="random", balanced=True)


# # Save set

def save_subset(subset, file):
    subset["language"] = "es"
    subset["comment"] = ""
    subset = subset[["language", "is_hate", "text", "tweet_id", "comment"]]
    subset.to_csv(OUTPUT_PATH, index=False, sep="|", encoding="utf-8")


save_subset(subset, OUTPUT_PATH)

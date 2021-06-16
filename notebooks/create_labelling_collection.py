# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.10.0
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

DATASET = "dataset_v03"
SIZE = 400
COLLECTION_NUM = "05"

SEED = 35

# ## Paths

DATASET_PATH = f"data/datasets/{DATASET}.pkl"
CLASSIFIED_DATASET_PATH = "data/datasets/dataset_label.pkl"
OUTPUT_PATH = f"data/labelling/collections/collection_{COLLECTION_NUM}.csv"

# # Retrieve dataset

dataset = read_data(DATASET_PATH)
dataset.head(5)

# # Assign pre-label

dataset_classified = read_data(CLASSIFIED_DATASET_PATH)
dataset_classified = dataset_classified[["tweet_id", "is_hate"]]
dataset_classified = dataset_classified.fillna("u")
dataset_classified = dataset_classified.replace([True, False], ["y", "n"])
dataset_classified.head(5)

dataset = dataset.merge(dataset_classified, on="tweet_id", how="left")


# # Generate labelling collection

def generate_collection(dataset, strategy, balanced):
    if strategy == "random":
        subset_yes = dataset[dataset["is_hate"] == "y"].sample(n=int(SIZE/2), random_state=SEED)
        subset_no = dataset[dataset["is_hate"] == "n"].sample(n=int(SIZE/2), random_state=SEED)
        collection = pd.concat([subset_yes, subset_no]).sample(frac=1)
    return collection


collection = generate_collection(dataset, strategy="random", balanced=True)


# # Save collection

def save_collection(collection, file):
    collection["language"] = "es"
    collection["comment"] = ""
    collection = collection[["language", "is_hate", "text", "tweet_id", "comment"]]
    collection.to_csv(OUTPUT_PATH, index=False, sep="|", encoding="utf-8")


save_collection(collection, OUTPUT_PATH)

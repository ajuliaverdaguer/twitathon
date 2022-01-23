# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.12.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# cd ..

# %load_ext autoreload
# %autoreload 2

# +
import numpy as np
import pandas as pd

from omegaconf import OmegaConf

from src.utils.utils import read_data, get_all_db_files, get_complete_table

import re
import unidecode

from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn import metrics
# -

paths = OmegaConf.load("config/paths.yaml")

# # Settings

DATASET = "dataset_v07"
SIZE = 500
COLLECTION_NUM = "20"

SEED = int(COLLECTION_NUM)


# # Functions

def clean_message(message):
    stop_words = set(stopwords.words("spanish"))
    message = message.lower()
    message = unidecode.unidecode(message)
    message = re.sub("[^a-zA-Z]+", " ", message)
    message = message.strip()
    message = re.sub(' +', ' ', message)
    message = " ".join([word for word in message.split(" ") if word not in stop_words])
    return message


# ## Paths

DATASET_PATH = f"data/datasets/{DATASET}.pkl"
HATE_SPEECH_WITH_LABELS_FILE = "data/labelling/hate_speech_with_labels.csv"
OUTPUT_PATH = f"data/labelling/collections/collection_{COLLECTION_NUM}.csv"

# # Retrieve dataset

dataset = read_data(DATASET_PATH)
dataset.head(5)

# # Assign pre-label

# ## Data

# ### Retrieve data

data = pd.read_csv(HATE_SPEECH_WITH_LABELS_FILE, sep="|")

known_ids = list(data["tweet_id"].astype(str))

# ## Prepare data

data = data[data["mean_label"].isin(["yes", "no"])]
data = data[["text", "mean_label"]]

data["text_clean"] = data["text"].apply(clean_message)
data["label_int"] = data["mean_label"].map({"no":0, "yes":1})

# ## Model

# ### Prepare data

# #### Define X and y

X = data["text_clean"]
y = data["label_int"]

# #### TF-IDF vectorizer

vectorizer = TfidfVectorizer()
data_vectorized = vectorizer.fit_transform(X)

# ## Model

# ### Instance

model = RandomForestClassifier(class_weight='balanced',
                               n_estimators=1000,
                               random_state=31)

# ### Training

model.fit(data_vectorized, y)

# ## Predictions

dataset["text_clean"] = dataset["text"].apply(clean_message)

X_dataset = dataset["text_clean"]
dataset_vectorized = vectorizer.transform(X_dataset)

predictions = model.predict(dataset_vectorized)

dataset["predictions"] = predictions

dataset["is_hate"] = "n"
dataset["is_hate"][dataset["predictions"] == 1] = "y"

dataset = dataset.drop(columns=["text_clean", "predictions"])

# ### Remove already labelled ids

dataset = dataset[~dataset["tweet_id"].isin(known_ids)]

dataset.head()


# # Generate labelling collection

def generate_collection(dataset):
    subset_yes = dataset[dataset["is_hate"] == "y"].sample(n=9*int(SIZE/10), random_state=SEED)
    subset_no = dataset[dataset["is_hate"] == "n"].sample(n=int(SIZE/10), random_state=SEED)
    collection = pd.concat([subset_yes, subset_no]).sample(frac=1)
    return collection


collection = generate_collection(dataset)


# # Save collection

def save_collection(collection, file):
    collection["language"] = "es"
    collection["comment"] = ""
    collection = collection[["language", "is_hate", "text", "tweet_id", "comment"]]
    collection.to_csv(OUTPUT_PATH, index=False, sep="|", encoding="utf-8")


save_collection(collection, OUTPUT_PATH)

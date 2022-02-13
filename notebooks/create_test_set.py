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

DATASET = "dataset_v09"
TEST_SET_NUM = "2"

SEED = int(TEST_SET_NUM)


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
OUTPUT_PATH = f"data/labelling/test_set/test_set_{TEST_SET_NUM}.csv"

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

len(dataset)


# # Generate test set

# +
def clean_text(text):
    text = re.sub(r'\W+', ' ', text)
    text = text.lower()
    text = re.sub(" +", " ", text).strip()
    return text

def is_word_in_text(word, text):
    words = text.split(" ")
    return word in words

def contains_racist_word(text):
    text = clean_text(text)
    for word in words_racist:
        if is_word_in_text(word, text):
            return True
    return False


# -

hashtags = pd.read_csv("data/entities/original_hashtags.csv")

words_racist = hashtags[hashtags["category"] == "racist"]["hashtag"].tolist()
words_racist = [clean_text(w) for w in words_racist]

test_set = dataset.copy()

test_set["contains_racist_word"] = test_set["text"].apply(contains_racist_word)

# ## Racist subset

# ### Containing words

size = 75

subset = test_set[(dataset["is_hate"] == "y") & (test_set["contains_racist_word"] == True)]

test_set_yes_racist_yes_word = subset.sample(n=size, random_state=SEED)

# ### No containing words

size = 75

subset = test_set[(dataset["is_hate"] == "y") & (test_set["contains_racist_word"] == False)]

test_set_yes_racist_no_word = subset.sample(n=size, random_state=SEED)

# ## Non-racist subset

# ### Containing words

size = 75

subset = test_set[(dataset["is_hate"] == "n") & (test_set["contains_racist_word"] == True)]

test_set_no_racist_yes_word = subset.sample(n=size, random_state=SEED)

# ### No containing words

size = 75

subset = test_set[(dataset["is_hate"] == "n") & (test_set["contains_racist_word"] == False)]

test_set_no_racist_no_word = subset.sample(n=size, random_state=SEED)

# # Save collection

test_set = pd.concat([test_set_yes_racist_yes_word, test_set_yes_racist_no_word, test_set_no_racist_yes_word, test_set_no_racist_no_word]).sample(frac=1).reset_index(drop=True)

test_set["language"] = "es"
test_set["comment"] = ""
test_set = test_set[["language", "is_hate", "contains_racist_word", "text", "tweet_id", "comment"]]

test_set.to_csv(OUTPUT_PATH, index=False, sep="|", encoding="utf-8")

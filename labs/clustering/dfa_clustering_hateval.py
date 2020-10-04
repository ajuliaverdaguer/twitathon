# -*- coding: utf-8 -*-
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

# +
import matplotlib.pyplot as plt
import numpy as np
import fasttext
import glob
import os
import pandas as pd
import re
import umap.umap_ as umap
import unidecode

from collections import defaultdict
from gensim.models.phrases import Phrases, Phraser
from sklearn.cluster import DBSCAN
from stop_words import get_stop_words
# -

# # Settings

HATEVAL_FILES = "../data/hateval/hateval2019_es_*.csv"


# # Functions

# +
def read_data(file):
    files = glob.glob(HATEVAL_FILES)
    df = list()
    for file in files:
        df.append(pd.read_csv(file))
    return pd.concat(df)    

def remove_extra_spaces(text):
    text = re.sub(" +", " ", text)
    return text.strip()

def remove_stopwords(text, stopwords):
    words = text.split(" ")
    words_valid = [w for w in words if w not in stopwords]
    return " ".join(words_valid)

def remove_punctuation(text, punctuation_list):
    regex = f"[{''.join(punctuation_list)}]+"
    return re.sub(regex, '', text)

def clean_text(text):
    stopwords = get_stop_words("en") + get_stop_words("es") + get_stop_words("ca")
    punctuation_list = [".", ",", ":", ";", "'", '"']
    text = remove_extra_spaces(text)
    text = text.lower()
    text = remove_stopwords(text, stopwords)
    text = unidecode.unidecode(text)
    text = remove_punctuation(text, punctuation_list)
    return text

def find_word_frequency(sentences):
    word_freq = defaultdict(int)
    for sent in sentences:
        for i in sent:
            word_freq[i] += 1
    return word_freq

def get_most_frequent_words(word_freq, n):
    return sorted(word_freq, key=word_freq.get, reverse=True)[:n]

def create_model(texts):
    temp_file = "temp.txt"
    with open(temp_file, "w") as f:
        f.write(texts.str.cat(sep='\n'))
    model = fasttext.train_unsupervised(temp_file, minn=2, maxn=5, dim=100)
    os.remove(temp_file)
    return model

def vectorize(model, line):
    words = list()
    for word in line:
        words.append(np.array(model[word]))
    if words:
        words = np.asarray(words)
        return words.mean(axis=0)
    if not words:
        return None
    
def analyse_cluster(df, cluster):
    print(f"CLUSTER {cluster}")
    df_cluster = df[df["cluster"] == cluster].reset_index(drop=True)
    print(f"- Cluster size: {len(df_cluster)} ({round(100 * len(df_cluster) / len(df))}%)")
    word_frequency = find_word_frequency(df_cluster["sentence"])
    top_10 = get_most_frequent_words(word_frequency, 10)
    print(f"- 10 most frequent words: {top_10}")
    df_cluster["contain_top_10"] = df_cluster["sentence"].apply(lambda x: (len(set(x)) != len(set(x)-set(top_10))))
    print(f"- Tweets containing 10 most frequent words: {round(100*  df_cluster['contain_top_10'].sum() / len(df_cluster))}%")
    print("")

def cluster_analysis(df):
    clusters = sorted(df["cluster"].unique().tolist())
    print(f"NUMBER OF CLUSTERS: {len(clusters)}")
    print("")
    for cluster in clusters:
        analyse_cluster(tweets_valid, cluster)


# -

# # Retrieve data

tweets = read_data(HATEVAL_FILES)
print(len(tweets))
tweets.head()

# # Pre-processing data

# ## Cleaning

# - Remove extra spaces
# - To lower
# - Remove stop words
# - Remove accents (alse removes ñs and çs)
# - Remove specific punctuation

tweets["text_clean"] = tweets["text"].apply(clean_text)
print(len(tweets))
tweets.head()

tweets = tweets.drop_duplicates(subset="text_clean")
len(tweets)

# ## Bigrams

sent = [row.split() for row in tweets["text_clean"]]

phrases = Phrases(sent, min_count=30, progress_per=10000)

bigram = Phraser(phrases)
sentences = bigram[sent]

tweets["sentence"] = sentences

# ## Analysis

# ### Most Frequent Words

word_freq = find_word_frequency(tweets["sentence"])
len(word_freq)

get_most_frequent_words(word_freq, 10)

# # Model

# # Model

# ## Create model

model = create_model(tweets["text_clean"])

# ## Analysis

model.get_nearest_neighbors("racismo")

# # Clustering

# ## Vectorize

tweets["vector"] = tweets["sentence"].apply(lambda x: vectorize(model, x))
tweets.head()

tweets["is_valid_vector"] = tweets["vector"].apply(lambda x: type(x) == np.ndarray)
tweets.head()

tweets_valid = tweets[tweets["is_valid_vector"] == True]
len(tweets_valid)

X = np.stack(tweets_valid["vector"], axis=0)

# ## Clustering model

embedding_2d = umap.UMAP(random_state=31).fit_transform(X)

dbscan = DBSCAN(metric="euclidean", eps=2, min_samples=1000)
cluster_labels = dbscan.fit_predict(embedding_2d)

clusters = pd.DataFrame(pd.Series(cluster_labels).value_counts(), columns=["counts"]).reset_index().rename(columns = {"index": "cluster"}).sort_values("cluster").reset_index().drop(columns="index")
clusters

tweets_valid["cluster"] = cluster_labels
tweets_valid.head()

# ## Visualization

classes = pd.Series(cluster_labels).unique().tolist()
plt.figure(figsize=(10,10))
scatter = plt.scatter(embedding_2d[:, 0], embedding_2d[:, 1], c=cluster_labels, s=0.1)
plt.legend(handles=scatter.legend_elements()[0], labels=classes)
plt.show()

# ## Cluster analysis

cluster_analysis(tweets_valid)

# # Based on
# - https://www.kaggle.com/pierremegret/gensim-word2vec-tutorial
# - https://umap-learn.readthedocs.io/en/latest/clustering.html

# # Ideas
#
# - Remove users and hashtags

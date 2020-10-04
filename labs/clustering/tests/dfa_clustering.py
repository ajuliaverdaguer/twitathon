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

import matplotlib.pyplot as plt
import multiprocessing
import numpy as np
import pandas as pd
import re
import spacy
import umap.umap_ as umap
import unidecode
from collections import defaultdict
from gensim.models import Word2Vec
from gensim.models.phrases import Phrases, Phraser
from sklearn.cluster import DBSCAN
from stop_words import get_stop_words
from time import time

# # Settings

TWEETS_FILE = "../data/tweets.pkl"


# # Functions

# +
def read_data(file):
    df = pd.read_pickle(file, compression="gzip")
    return df

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


# -

# # Retrieve data

tweets = read_data(TWEETS_FILE)

tweets = read_data(TWEETS_FILE)
print(len(tweets))
tweets.head()

tweets = tweets[tweets["type"] == "regular"][["text"]].reset_index(drop=True)
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

# Drop duplicates?

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

# ## Create model

cores = multiprocessing.cpu_count()

w2v_model = Word2Vec(min_count=20,
                     window=2,
                     size=300,
                     sample=6e-5, 
                     alpha=0.03, 
                     min_alpha=0.0007, 
                     negative=20,
                     workers=cores-1)

# ## Build vocabulary

w2v_model.build_vocab(tweets["sentence"], progress_per=10000)

# ## Train model

w2v_model.train(tweets["sentence"], total_examples=w2v_model.corpus_count, epochs=30, report_delay=1)

w2v_model.init_sims(replace=True)

# ## Analysis

# ### Similar words

# + active=""
# w2v_model.wv.vocab
# -

w2v_model.wv.most_similar(positive=["pablo_iglesias"])

w2v_model.wv.most_similar(positive=["pablo_iglesias", "psoe"], negative=["podemos"], topn=3)

# # Clustering

# ## Vectorize

w2v_vectors = w2v_model.wv.vectors # here you load vectors for each word in your model
w2v_indices = {word: w2v_model.wv.vocab[word].index for word in w2v_model.wv.vocab} # here you load indices - with whom you can find an index of the particular word in your model 


# +
def vectorize(line): 
    words = []
    for word in line: # line - iterable, for example list of tokens 
        try:
            w2v_idx = w2v_indices[word]
        except KeyError: # if you does not have a vector for this word in your w2v model, continue 
            continue
        words.append(w2v_vectors[w2v_idx])
    if words: 
        words = np.asarray(words)
        min_vec = words.min(axis=0)
        max_vec = words.max(axis=0)
        return np.concatenate((min_vec, max_vec))
    if not words:
        return None 
    
#def vectorize(line):
#    words = []
#    for word in line: # line - iterable, for example list of tokens 
#        try:
#            w2v_idx = w2v_indices[word]
#        except KeyError: # if you does not have a vector for this word in your w2v model, continue 
#            continue
#        words.append(w2v_vectors[w2v_idx])
#    if words: 
#        words = np.asarray(words)
#        return words.mean(axis=0)
#    if not words:
#        return None 


# -

vectorize(tweets["sentence"][0]).shape

tweets["vector"] = tweets["sentence"].apply(vectorize)
tweets.head()

tweets["is_valid_vector"] = tweets["vector"].apply(lambda x: type(x) == np.ndarray)
tweets.head()

tweets_valid = tweets[tweets["is_valid_vector"] == True]
len(tweets_valid)

X = np.stack(tweets_valid["vector"], axis=0)

# ## Clustering model

dbscan = DBSCAN(metric="cosine", eps=0.07, min_samples=3) # you can change these parameters, given just for example 
cluster_labels = dbscan.fit_predict(X) # where X - is your matrix, where each row corresponds to one document (line) from the docs, you need to cluster 

clusters = pd.DataFrame(pd.Series(cluster_labels).value_counts(), columns=["counts"]).reset_index().rename(columns = {"index": "cluster"}).sort_values("cluster").reset_index().drop(columns="index")
clusters

tweets_valid["cluster"] = cluster_labels
tweets_valid.head()

# ## Visualization

standard_embedding = umap.UMAP(random_state=31).fit_transform(X)

plt.scatter(standard_embedding[:, 0], standard_embedding[:, 1], c=cluster_labels, s=0.1)

# ## Cluster analysis

tweets_valid.head()


# +
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

cluster_analysis(tweets_valid)

# + active=""
# NUM = 5
# for cluster in clusters["cluster"].unique():
#     print("")
#     print(f"CLUSTER {cluster}")
#     texts_cluster = tweets_valid[tweets_valid["cluster"] == cluster]["text"].tolist()
#     for i in range(min([NUM, len(texts_cluster)])):
#         print("")
#         print(texts_cluster[i])
#     print("")
#     print("---------------")
# -

# # Based on
# - https://www.kaggle.com/pierremegret/gensim-word2vec-tutorial
# - https://umap-learn.readthedocs.io/en/latest/clustering.html

# # Ideas
# - Iterate clustering.
# - Remove duplicates

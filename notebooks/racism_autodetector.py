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
import pandas as pd
import re
import unidecode

from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn import metrics
# -

# # Settings

DATA_FILE = "data/labelling/hate_speech_with_labels.csv"


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


# # Data

# ## Retrieve data

data = pd.read_csv(DATA_FILE, sep="|")

# ## Prepare data

data = data[data["mean_label"].isin(["yes", "no"])]
data = data[["text", "mean_label"]]

data["text_clean"] = data["text"].apply(clean_message)
data["label_int"] = data["mean_label"].map({"no":0, "yes":1})

data.head()

# # Model

# ## Prepare data

# ### Define X and y

X = data["text_clean"]
y = data["label_int"]

# ### TF-IDF vectorizer

vectorizer = TfidfVectorizer()
data_vectorized = vectorizer.fit_transform(X)

df = pd.DataFrame(data_vectorized.toarray(), columns=vectorizer.get_feature_names())
df.head()

# ### Train - test split

TEST_SIZE = 0.2
X_train, X_test, y_train, y_test = train_test_split(data_vectorized, y, test_size=TEST_SIZE, random_state=31)

# ## Model

# ### Instance

model = RandomForestClassifier(class_weight='balanced',
                               n_estimators=1000,
                               random_state=31)

# ### Training

model.fit(X_train, y_train)

# ### Evaluation

predictions = model.predict(X_test)

print(metrics.classification_report(y_test, predictions))

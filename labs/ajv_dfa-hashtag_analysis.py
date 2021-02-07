# # Hashtag analysis

# ## Settings  

from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv_dfa-first_draft')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

import pandas as pd

data_path = Path(source_path).parent / 'data'

entities_file = data_path / 'entities_to_retrieve_sample.txt'

hashtags_file = data_path / 'hashtags.csv'
tweets_file = data_path / 'tweets.csv'

# ## Retrieve information

selected_lang = ['ca', 'es']

tweets = pd.read_csv(tweets_file, sep=';')

tweets_ids = tweets[tweets['lang'].isin(selected_lang)]['tweet_id'].to_list()

if len(tweets_ids) != len(set(tweets_ids)):
    
    print("Beware!!! There may be duplicate tweets!!!")

# ### Hashtags

hashtags = pd.read_csv(hashtags_file, sep=';')
hashtags['hashtag'] = hashtags['hashtag'].str.lower()
hashtags = hashtags[hashtags['tweet_id'].isin(tweets_ids)]

hashtags.head()

# ## Hashtag distribution

hashtags.hashtag.value_counts().to_frame()

downloaded = [h[1:].lower() for h in pd.read_csv(entities_file, header=None)[0] if h.startswith('#')]

hashtags[~hashtags['hashtag'].isin(downloaded)].hashtag.value_counts().to_frame().head(20)



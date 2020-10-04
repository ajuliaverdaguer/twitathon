# # Language detection

from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv-detect_language')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

from langdetect import detect, lang_detect_exception
import pandas as pd

data_path = Path(source_path).parent / 'data'

data = pd.read_pickle(data_path / 'tweets.pkl', compression="gzip")

subset = data[['tweet_id', 'text', 'lang']][10000:20000]

subset.groupby('lang').size().to_frame().rename(columns={0: 'count'}).\
    sort_values('count', ascending=False).head(5)

subset.shape

subset['lang_detect'] = subset['text'].apply(utils.detect_text_language)

subset[subset['lang_detect'].isna()]

sum(subset['lang'] == subset['lang_detect'])

for i, row in subset[subset['lang'] != subset['lang_detect']].iterrows():
    print(row['text'])
    print(row['lang'])
    print(row['lang_detect'])
    print("-------------")



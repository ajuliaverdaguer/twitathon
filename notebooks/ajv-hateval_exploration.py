# # Hateval exploration

# https://www.aclweb.org/anthology/S19-2007.pdf

from pathlib import Path
import os
import sys
source_path = str(Path(os.path.abspath('ajv-hateval_exploration')).parent.parent / 'src')
if source_path not in sys.path:
    sys.path.insert(0, source_path)

import pandas as pd

data_path = Path(source_path).parent / 'data' / 'hateval'

[str(f) for f in data_path.glob('*')]

esp_files = [str(f) for f in data_path.glob('*') if '_es_' in str(f)]

train = pd.read_csv(esp_files[1])

train.shape

train.head()

# Target categories:
# * **HS** - a binary value indicating if HS is occurring against one of the given targets (women
# or immigrants): 1 if occurs, 0 if not.
# * **Target Range** - if HS occurs (i.e. the value
# for the feature HS is 1), a binary value indicating if the target is a generic group of people (0) or a specific individual (1).
# * **Aggressiveness** - if HS occurs (i.e. the value
# for the feature HS is 1), a binary value indicating if the tweeter is aggressive (1) or
# not (0).

train.groupby('HS')[['text']].count() / train.shape[0] * 100

train.groupby(['HS', 'TR'])[['text']].count() / train.shape[0] * 100

train.groupby(['HS', 'AG'])[['text']].count() / train.shape[0] * 100

dev = pd.read_csv(esp_files[0])

dev.shape

dev.groupby('HS')[['text']].count() / dev.shape[0] * 100



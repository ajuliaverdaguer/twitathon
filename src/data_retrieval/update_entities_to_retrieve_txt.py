"""
This process aims at automating the update of the file 'entities_to_retrive.txt',
 used for downloading tweets from Twitter.
"""

from __future__ import print_function

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

import fire
import pandas as pd

from utils.paths import PATH_ORIGINAL_USERS, PATH_ORIGINAL_HASHTAGS, PATH_ENTITIES_TO_RETRIEVE


def update_entities_to_retrieve():
    """
    Read users and hashtags from files coming from Drive spreadsheet and merge them together in the
        entities_to_retrieve.txt file
    """

    users = pd.read_csv(PATH_ORIGINAL_USERS, usecols=['username']).rename(columns={'username': 'entity'})
    hashtags = pd.read_csv(PATH_ORIGINAL_HASHTAGS, usecols=['hashtag']).rename(columns={'hashtag': 'entity'})

    pd.concat([users, hashtags]).to_csv(PATH_ENTITIES_TO_RETRIEVE, header=False, index=False)
    # TODO: Change 'entities_to_retrieve_test' to 'entities_to_retrieve' once we want to put this in "production"


if __name__ == '__main__':
    # python src/update_entities_to_retrieve_txt.py update_entities_to_retrieve
    fire.Fire()

    # For debugging
    # update_entities_to_retrieve()

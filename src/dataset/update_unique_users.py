"""
Script aiming at updating a file linking original users with their corresponding Twitter ID
"""

import datetime as dt
import fire
import gzip
import numpy as np
import pandas as pd
from pathlib import Path
import pickle5 as pickle
import re

from utils.paths import PATH_FOLDER_RAW, PATH_ORIGINAL_USERS, PATH_ORIGINAL_USERS_IDS

RE_FILES = r"202(\d{1})-(\d{2})_users.pkl"


def check_all_user_files():

    return [f for f in Path(PATH_FOLDER_RAW).glob('*') if re.search(RE_FILES, str(f))]


def check_current_user_files():

    return Path(PATH_FOLDER_RAW) / f"{dt.date.today().year}-{str(dt.date.today().month).zfill(2)}_users.pkl"


def read_data(file):

    with gzip.open(file, "rb") as fh:
        data = pickle.load(fh)
    return data


def update_original_users_id():

    if not Path(PATH_ORIGINAL_USERS_IDS).exists():  # If never generated, check all user pickle files

        l = []
        for f in check_all_user_files():
            l.append(read_data(f)[['user_id', 'screen_name']].drop_duplicates())

        users = pd.concat(l).drop_duplicates()

    else:  # Else, check only last file with most recent users
        users = read_data(check_current_user_files())[['user_id', 'screen_name']].drop_duplicates()

    users['screen_name'] = users['screen_name'].apply(lambda x: x.lower())

    # Load a priori dataset of users
    original_users = pd.read_csv(PATH_ORIGINAL_USERS)[['username', 'category']]
    original_users['username'] = original_users['username'].apply(lambda x: x.replace('@', '').lower())

    # Merge by username and return for each user_id, the corresponding a priori category
    output = pd.merge(original_users, users, how='left', left_on='username', right_on='screen_name')

    # Remove users without user identifier
    output = output[~pd.isna(output.user_id)]

    if Path(PATH_ORIGINAL_USERS_IDS).exists():

        existent = pd.read_csv(PATH_ORIGINAL_USERS_IDS, dtype={'user_id': np.int64})
        output = pd.concat([existent, output])

    output['user_id'] = output['user_id'].astype(int)
    output[['user_id', 'username', 'category']].drop_duplicates().to_csv(PATH_ORIGINAL_USERS_IDS, index=False)


if __name__ == '__main__':
    # python src/update_unique_users.py update_original_users_id
    fire.Fire()

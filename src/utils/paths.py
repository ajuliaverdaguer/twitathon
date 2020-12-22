import time
import os

from pathlib import Path
from utils import utils

# Baseline
PATH_BASE = Path(os.path.abspath(__file__)).parent.parent.parent

# Paths dict
PATHS = utils.load_paths()['default']

# Config
PATH_CONFIG = PATH_BASE / PATHS['config']['config']
PATH_TOKEN = PATH_BASE / PATHS['config']['token_drive']
PATH_CREDENTIALS_DRIVE = PATH_BASE / PATHS['config']['credentials_drive']

# Data - Entities
PATH_ENTITIES_TO_RETRIEVE = PATH_BASE / PATHS['data']['entities_to_retrieve']
PATH_ENTITIES_AUTOMATIC = PATH_BASE / PATHS['data']['entities_automatic']
PATH_ENTITIES_AUTOMATIC_PICKLE = PATH_BASE / PATHS['data']['entities_automatic_pickle']
PATH_ORIGINAL_HASHTAGS = PATH_BASE / PATHS['data']['original_hashtags']
PATH_ORIGINAL_USERS = PATH_BASE / PATHS['data']['original_users']
PATH_ORIGINAL_USERS_IDS = PATH_BASE / PATHS['data']['original_users_ids']

# Data - Raw downloads
PATH_USERS_RAW = PATH_BASE / PATHS['data']['users_raw']
PATH_HASHTAGS_RAW = PATH_BASE / PATHS['data']['hashtags_raw']
PATH_TWEETS_RAW = PATH_BASE / PATHS['data']['tweets_raw']
PATH_MENTIONS_RAW = PATH_BASE / PATHS['data']['mentions_raw']
PATH_LOG = PATH_BASE / PATHS['data']['log']


# Data - Raw downloads current
def include_month_prefix(original_str):
    original_path = Path(original_str)

    month_prefix = time.strftime("%Y-%m")

    return str(Path(original_path.parent) / f"{month_prefix}_{original_path.name}")


PATH_USERS_RAW_CURRENT = PATH_BASE / include_month_prefix(PATHS['data']['users_raw'])
PATH_HASHTAGS_RAW_CURRENT = PATH_BASE / include_month_prefix(PATHS['data']['hashtags_raw'])
PATH_TWEETS_RAW_CURRENT = PATH_BASE / include_month_prefix(PATHS['data']['tweets_raw'])
PATH_MENTIONS_RAW_CURRENT = PATH_BASE / include_month_prefix(PATHS['data']['mentions_raw'])
PATH_LOG_CURRENT = PATH_BASE / include_month_prefix(PATHS['data']['log'])

"""
Find popular entities
"""

import datetime
import pandas as pd

from utils import utils
from utils.paths import PATH_USERS_RAW_CURRENT, PATH_TWEETS_RAW_CURRENT, PATH_ENTITIES_AUTOMATIC_PICKLE, \
    PATH_LOG_CURRENT

TODAY = datetime.date.today()
TOP_NUMBER = 10


def read_entities_automatic(file):
    try:
        df = utils.read_data(file)
    except FileNotFoundError:
        df = pd.DataFrame(columns=["entity_name", "entity_type", "date_top", "added_at"])
    return df


def get_top_entities_today(df, entity_name, number_of_entities=10):
    df[df["downloaded_at"].dt.date == TODAY]
    top_entities = list(df[entity_name].value_counts().index[:number_of_entities])
    top_entities_df = pd.DataFrame({"entity_name": top_entities})
    top_entities_df["entity_type"] = entity_name
    top_entities_df["date_top"] = TODAY
    top_entities_df["added_at"] = datetime.datetime.now()
    return top_entities_df


def save_txt_file(file, entities_list):
    with open(file, "w") as f:
        for item in entities_list:
            f.write("%s\n" % item)


def log_changes(top_hashtags_df, top_users_df):
    if len(top_hashtags_df) > 0:
        utils.log_and_print(f"Adding new hashtags to retrieve ({len(top_hashtags_df)})", PATH_LOG_CURRENT)
    if len(top_users_df) > 0:
        utils.log_and_print(f"Adding new users to retrieve ({len(top_users_df)})", PATH_LOG_CURRENT)


def update_entities_automatic(entities_to_retrieve_file, entities_automatic_txt_file):
    # Retrieve data
    print("- Retrieving data.")

    # Users
    users_df = utils.read_data(PATH_USERS_RAW_CURRENT)
    users_df = users_df[["user_id", "screen_name"]]
    users_df = users_df.rename(columns={"screen_name": "user"})

    # Tweets
    tweets_df_full = utils.read_data(PATH_TWEETS_RAW_CURRENT)
    tweets_df = tweets_df_full[["user_id", "downloaded_at"]]

    # Entities to retrieve
    entities_to_retrieve = utils.read_entities_to_retrieve_file(entities_to_retrieve_file)

    # Entities automatic
    entities_automatic_df = read_entities_automatic(PATH_ENTITIES_AUTOMATIC_PICKLE)

    # Get top entites
    print("- Getting top entities.")

    hashtags_list = pd.Series([word[1:] for text in tweets_df_full["text"] for word in text.split(" ") if
                               ((len(word) > 0) and (word[0] == "#"))])
    top_hashtags = list(hashtags_list.value_counts()[0:TOP_NUMBER].index)
    top_hashtags_df = pd.DataFrame({"entity_name": top_hashtags})
    top_hashtags_df["entity_type"] = "hashtag"
    top_hashtags_df["date_top"] = TODAY
    top_hashtags_df["added_at"] = datetime.datetime.now()

    users_df = tweets_df.merge(users_df, on="user_id", how="left").drop(columns=["user_id"])
    top_users_df = get_top_entities_today(users_df, "user", TOP_NUMBER)

    # Filter entities in entities to retrieve
    print("- Filtering entities in entities to retrieve.")

    # Hashtags
    entities_to_retrieve_hashtags = [entity[1:].lower() for entity in entities_to_retrieve if entity[0] == "#"]
    top_hashtags_df = top_hashtags_df[~top_hashtags_df["entity_name"].str.lower().isin(entities_to_retrieve_hashtags)]

    # Users
    entities_to_retrieve_users = [entity[1:].lower() for entity in entities_to_retrieve if entity[0] == "@"]
    top_users_df = top_users_df[~top_users_df["entity_name"].str.lower().isin(entities_to_retrieve_users)]

    # Filter entities already in automatic entities
    print("- Filtering entities in automatic entities.")

    # Hashtags
    hashtags_automatic_df = entities_automatic_df[entities_automatic_df["entity_type"] == "hashtag"]
    top_hashtags_df = top_hashtags_df[
        ~top_hashtags_df["entity_name"].str.lower().isin(hashtags_automatic_df["entity_name"].str.lower())]

    # Users
    users_automatic_df = entities_automatic_df[entities_automatic_df["entity_type"] == "user"]
    top_users_df = top_users_df[
        ~top_users_df["entity_name"].str.lower().isin(users_automatic_df["entity_name"].str.lower())]

    # Save files
    print("- Saving files.")

    # Pickle
    entities_automatic_df_updated = pd.concat([entities_automatic_df, top_hashtags_df, top_users_df]).reset_index(
        drop=True)
    entities_automatic_df_updated.to_pickle(PATH_ENTITIES_AUTOMATIC_PICKLE, compression="gzip")

    # Txt
    hashtags_list = [f"#{hashtag}" for hashtag in entities_automatic_df_updated["entity_name"][
        entities_automatic_df_updated["entity_type"] == "hashtag"]]
    users_list = [f"@{user}" for user in
                  entities_automatic_df_updated["entity_name"][entities_automatic_df_updated["entity_type"] == "user"]]
    save_txt_file(entities_automatic_txt_file, hashtags_list + users_list)

    # Log
    log_changes(top_hashtags_df, top_users_df)

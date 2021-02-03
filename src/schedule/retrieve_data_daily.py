"""
This script aims at retrieving
"""

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent.parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from data_retrieval.retrieve_tweets import retrieve_tweets_from_file
from data_retrieval.automatically_download_hashtags_users_sheet import retrieve_users_hashtags
from data_retrieval.update_entities_to_retrieve_txt import update_entities_to_retrieve
from data_retrieval.update_entities_automatic import update_entities_automatic

from utils.paths import PATH_ENTITIES_TO_RETRIEVE, PATH_ENTITIES_AUTOMATIC

# SETTINGS
NUMBER_OF_TWEETS = 1000

def main():

    print("\n############################################")
    print("THE TWITATHON PROJECT - DAILY DATA RETRIEVAL")
    print("############################################")

    print("\nUPDATING MANUAL LIST OF HASHTAGS AND USERS TO RETRIEVE...\n")
    #retrieve_users_hashtags()
    #update_entities_to_retrieve()

    print("\nRETRIEVING TWEETS FROM MANUAL LIST...\n")
    #retrieve_tweets_from_file(file=PATH_ENTITIES_TO_RETRIEVE, number_of_tweets=NUMBER_OF_TWEETS)

    print("\nUPDATING AUTOMATIC LIST OF HASHTAGS AND USERS TO RETRIEVE...\n")
    update_entities_automatic(PATH_ENTITIES_TO_RETRIEVE, PATH_ENTITIES_AUTOMATIC)

    print("\nRETRIEVING TWEETS FROM AUTOMATIC LIST...\n")
    retrieve_tweets_from_file(file=PATH_ENTITIES_AUTOMATIC, number_of_tweets=NUMBER_OF_TWEETS)


if __name__ == '__main__':
    main()

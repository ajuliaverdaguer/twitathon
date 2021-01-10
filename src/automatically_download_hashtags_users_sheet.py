"""
This process aims at automatically downloading new hashtags and users from our Google spreadsheets.
"""

from __future__ import print_function

from pathlib import Path
import os
import sys

source_path = str(Path(os.path.abspath(__file__)).parent)
if source_path not in sys.path:
    sys.path.insert(0, source_path)

from utils import utils

import fire
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging
import os.path
import pandas as pd
import pickle

from utils.paths import PATH_ORIGINAL_USERS, PATH_TOKEN, PATH_CREDENTIALS_DRIVE, PATH_ORIGINAL_HASHTAGS, \
    PATH_ORIGINAL_USERS_IDS

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Cells ranges for users and hashtags
USERS_RANGE = 'Users!A:E'
HASHTAGS_RANGE = 'Hashtags!A:C'


def retrieve_from_spreadsheet(spreadsheet_id, cells_range, save_path):
    """
    Retrieve data from Google Sheets spreadsheet, for a specific range of cells and save it into an output file

    Parameters
    ----------
    spreadsheet_id: str
        Spreadsheet ID
    cells_range: str
        Range of cells to save
    save_path: pathlib.Path
        Output saving path
    """

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(PATH_TOKEN):
        with open(PATH_TOKEN, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                PATH_CREDENTIALS_DRIVE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(PATH_TOKEN, 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=cells_range).execute()
    values = result.get('values', [])

    if not values:
        logging.error(f'No data found to save in {save_path}.')
    else:
        logging.info(f'Saving to {save_path}')

        output = pd.DataFrame(values[1:])
        output.columns = [c.lower() for c in values[0]]

        output.to_csv(save_path, index=False)


def retrieve_users_hashtags():
    """
    Retrieve list of users and hashtags from "Hashtags and users" spreadsheet. This file contains one tab for
        Users and another one for Hashtags, and it is updated manually, following a predefined structure
    """

    # Get spreadsheet_id from config file
    spreadsheet_id = utils.load_config()['default']['drive']['spreadsheet_id']

    retrieve_from_spreadsheet(spreadsheet_id, USERS_RANGE, PATH_ORIGINAL_USERS)
    retrieve_from_spreadsheet(spreadsheet_id, HASHTAGS_RANGE, PATH_ORIGINAL_HASHTAGS)


if __name__ == '__main__':
    # python src/automatically_download_hashtags_users_sheet.py retrieve_users_hashtags
    fire.Fire()

    # For debugging
    # retrieve_users_hashtags()

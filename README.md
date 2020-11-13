# The Twitathon project

This code is used to connect to the Twitter API and download tweets associated with a predefined list of hashtags and users.

## Handling requirements

Create a new virtual environment (optional):

`virtualenv -p python3 .venv`

Enter the environment:

`source .venv/bin/activate`

Install requirements:

`pip install -r requirements.txt`

## Defining list of hashtags and users

The list of hashtags and users for which we want to retrieve tweets must be written in a new file called `data/entities_to_retrieve.txt`, which must have the same structure than the `data/entities_to_retrieve_sample.txt` file included in the repo.

## Launching the code

To initiate the tweet retrieval, one must run the following instruction from the root of the repo:

`python src/retrieve_tweets.py retrieve_tweets_from_file --file='data/entities_to_retrieve.txt' --number_of_tweets=1000`

Note that the number of tweets can be changed.

## Cron

The instruction that has to be included in the cron (including entering the virtual environment) is:

`cd twitathon && /home/pi/twitathon/.venv/bin/python src/retrieve_tweets.py retrieve_tweets_from_file --file='data/entities_to_retrieve.txt' --number_of_tweets=1000`

## Data

Data is stored within the `data` folder following the sub-folder structure described below.

- **`raw`** Data retrieved from Twitter.
- **`dataset`** Files containing only id and processed message.

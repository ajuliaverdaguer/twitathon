CREATE TABLE IF NOT EXISTS tweets
    (tweet_id TEXT PRIMARY KEY,
    created_at TEXT,
    text TEXT,
    source TEXT,
    in_reply_to_status_id TEXT,
    user_id TEXT,
    geo TEXT,
    coordinates TEXT,
    place TEXT,
    contributors TEXT
    retweet_count INTEGER,
    favorite_count INTEGER,
    favorited TEXT,
    retweeted TEXT,
    lang TEXT,
    type TEXT,
    downloaded_at TEXT)
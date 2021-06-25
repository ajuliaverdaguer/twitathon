# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.10.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Notebook created following https://www.sqlitetutorial.net/

import sqlite3
from sqlite3 import Error

# # Settings

DATABASE_FILE = "2021-06.db"


# # Create database

def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        return connection
    except Error as error:
        print(error)
    return connection


# # Create tables

def create_table(connection, create_table_sql):
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
    except Error as error:
        print(error)


def create_all_tables(db_file):
    connection = create_connection(db_file)
    if connection is not None:
        tables = ["tweets", "users", "mentions"]
        for table in tables:
            query = open(f"sql/create_table_{table}.sql").read()
            create_table(connection, query)
            print(f"{table} table created.")
    else:
        print("Error! cannot create the database connection.")


create_all_tables(DATABASE_FILE)


# # Delete rows

def delete_all_mentions(connection):
    sql = 'DELETE FROM mentions'
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()


connection = create_connection(DATABASE_FILE)
delete_all_mentions(connection)


# # Insert

def insert_mention(connection, tweet_id, user_id, timestamp):
    cursor = connection.cursor()
    sql = "INSERT INTO mentions(tweet_id, user_id, downloaded_at) VALUES(?,?,?)"
    values = (tweet_id, user_id, timestamp)
    cursor.execute(sql, values)
    connection.commit()


connection = create_connection(DATABASE_FILE)
with connection:
    insert_mention(connection, "a", "1", "x")
    insert_mention(connection, "b", "2", "y")
    insert_mention(connection, "c", "3", "z")


# # Select

def select_everything(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM mentions")

    rows = cursor.fetchall()

    for row in rows:
        print(row)


connection = create_connection(DATABASE_FILE)
select_everything(connection)

# # Reading data with Pandas

import pandas as pd

conectionn = sqlite3.connect(DATABASE_FILE)
df = pd.read_sql_query("SELECT * from mentions", connection)
df

# # Writing using Pandas

df_new = pd.DataFrame({"tweet_id": ["a", "b", "d"], "user_id": [1, 2, 4], "downloaded_at": [55, 44, 66]})

df_new.to_sql("aa", connection, index = False, if_exists="append")

connection = sqlite3.connect(DATABASE_FILE)
df = pd.read_sql_query("SELECT * from aa", connection)
df


# # Update mentions

# +
def select_id_field(connection, table, id_field):
    query = open(f"sql/select_id_field.sql").read().format(id_field=id_field, table=table)
    df = pd.read_sql_query(query, connection)
    return df[id_field].tolist()

def delete_based_on_id_list(connection, table, id_field, id_list):
    question_marks = ", ".join("?" * len(id_list))
    query = open(f"sql/delete_based_on_id_list.sql").read().format(table=table, id_field=id_field, id_list=question_marks)
    cursor = connection.cursor()
    cursor.execute(query, id_list)
    connection.commit()

def get_list_of_tables(connection):
    cursor = connection.cursor()
    cursor.execute(open(f"sql/get_list_of_tables.sql").read())
    tables = cursor.fetchall()
    return [name[0] for name in tables]

def update_table(db_file, table, id_field, df):
    connection = create_connection(db_file)
    with connection:
        if table in get_list_of_tables(connection):
            existing_ids = select_id_field(connection, table, id_field)
            new_ids = df[id_field].tolist()
            ids_to_remove = list(set(new_ids).intersection(set(existing_ids)))
            delete_based_on_id_list(connection, table, id_field, ids_to_remove)
        df.to_sql(table, connection, index = False, if_exists="append")


# -

connection = sqlite3.connect(DATABASE_FILE)
df = pd.read_sql_query("SELECT * from mentions", connection)
df

df_new

update_table(DATABASE_FILE, "mentions", "tweet_id", df_new)

connection = sqlite3.connect(DATABASE_FILE)
df = pd.read_sql_query("SELECT * from mentions", connection)
df

# # Actual DB

ACTUAL_DB_PATH = "../data/raw/2021-06.db"

connection = create_connection(ACTUAL_DB_PATH)
get_list_of_tables(connection)


def read_table(connection, table):
    return pd.read_sql_query(f"SELECT * from {table}", connection)


read_table(connection, "users")

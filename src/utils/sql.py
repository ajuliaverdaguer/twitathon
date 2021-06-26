import datetime
import pandas as pd
import sqlite3

from sqlite3 import Error


def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        return connection
    except Error as error:
        print(error)
    return connection


def select_id_field(connection, table, id_field):
    query = open(f"src/utils/sql/select_id_field.sql").read().format(id_field=id_field, table=table)
    df = pd.read_sql_query(query, connection)
    return df[id_field].tolist()


def delete_based_on_id_list(connection, table, id_field, id_list):
    question_marks = ", ".join("?" * len(id_list))
    query = open(f"src/utils/sql/delete_based_on_id_list.sql").read().format(table=table, id_field=id_field,
                                                                             id_list=question_marks)
    cursor = connection.cursor()
    cursor.execute(query, id_list)
    connection.commit()


def get_list_of_tables(connection):
    cursor = connection.cursor()
    cursor.execute(open(f"src/utils/sql/get_list_of_tables.sql").read())
    tables = cursor.fetchall()
    return [name[0] for name in tables]


def update_table(db_file, table, id_field, df):
    df["downloaded_at"] = datetime.datetime.now()
    connection = create_connection(db_file)
    with connection:
        if table in get_list_of_tables(connection):
            existing_ids = select_id_field(connection, table, id_field)
            new_ids = df[id_field].tolist()
            ids_to_remove = list(set(new_ids).intersection(set(existing_ids)))
            delete_based_on_id_list(connection, table, id_field, ids_to_remove)
        df.to_sql(table, connection, index=False, if_exists="append")

def read_table(db_file, table):
    connection = create_connection(db_file)
    with connection:
        df = pd.read_sql(f"SELECT * FROM {table}", con=connection)
    return df
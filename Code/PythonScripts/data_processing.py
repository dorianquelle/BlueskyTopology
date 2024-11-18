"""
This script processes and uploads Bluesky social network data to an SQL database.

Main functions:

create_data_dict(file: str) -> dict:
    Reads a gzipped JSON file containing Bluesky data and organizes it into a dictionary.
    Input: Filename of the gzipped JSON file.
    Output: Dictionary with Bluesky data types as keys and lists of entries as values.

upload_data_to_sql(conn: psycopg2.extensions.connection, data: dict, verbose: bool = True) -> None:
    Uploads processed Bluesky data to the SQL database.
    Input: 
        conn: SQL database connection
        data: Dictionary of Bluesky data
        verbose: Boolean to control print statements
    Output: None (data is uploaded to the database)

process_and_upload_file(file: str) -> None:
    Orchestrates the process of reading a file, processing its data, and uploading to the database.
    Input: Filename of the gzipped JSON file to process.
    Output: None (data is processed and uploaded to the database)

Data processing functions:
    posts, profiles, follows, likes, reposts, blocks, listblock, create_list, listitem, threadgate, feed_generator
    These functions process specific types of Bluesky data and return pandas DataFrames.

Helper functions:
    apply_if_type, get_quote_info: Assist in data processing and extraction.

The script also includes SQL queries to create necessary database tables and functions to handle data type conversions and uploads.
"""

import os
import pandas as pd
import psycopg2
import json
import gzip
from collections import Counter
from tqdm import tqdm
from psycopg2.extras import execute_values
import numpy as np
from multiprocessing import Pool


def feed_generator(x):
    df = pd.DataFrame(x["app.bsky.feed.generator"])
    
    df["skyfeedBuilder_id_input"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["id"] for z in y["blocks"] if z["type"] == "input"]))
    )
    df["skyfeedBuilder_id_regex"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["id"] for z in y["blocks"] if z["type"] == "regex"]))
    )
    df["skyfeedBuilder_id_remove"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["id"] for z in y["blocks"] if z["type"] == "remove"]))
    )
    df["skyfeedBuilder_id_sort"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["id"] for z in y["blocks"] if z["type"] == "sort"]))
    )
    df["skyfeedBuilder_value_regex"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["value"] for z in y["blocks"] if z["type"] == "regex"]))
    )
    df["skyfeedBuilder_sort_type"] = df["skyfeedBuilder"].apply(
        lambda x: apply_if_type(x, lambda y: ", ".join([z["sortType"] for z in y["blocks"] if z["type"] == "sort"]))
    )
    
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)

    df = df.loc[:,["cid_entry","did", "createdAt", "description", "displayName", "skyfeedBuilder_id_input", "skyfeedBuilder_id_regex", "skyfeedBuilder_id_remove", "skyfeedBuilder_id_sort", "skyfeedBuilder_value_regex", "skyfeedBuilder_sort_type"]]
    return df

def apply_if_type(value, func, default=''):
    if isinstance(value, float) and np.isnan(value):
        return default
    try:
        return func(value)
    except (TypeError, KeyError, AttributeError) as e:
        return default
    
def profiles(x):
    df = pd.DataFrame(x["app.bsky.actor.profile"]).loc[:,["cid_entry",'description', 'displayName', 'did', 'createdAt']]
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    return df

def follows(x):
    df = pd.DataFrame(x["app.bsky.graph.follow"]).loc[:,["cid_entry",'subject', 'did', 'createdAt']]
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    return df

def likes(x):
    df = pd.DataFrame(x["app.bsky.feed.like"])
    df["subject_cid"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y.get("cid")))
    df["subject_uri"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y.get("uri")))
    df["subject"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y["uri"].split("/")[2]))
    df = df.loc[:,["cid_entry","did", "subject", "subject_cid", "subject_uri", "createdAt"]]
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    return df

def reposts(x):
    df = pd.DataFrame(x["app.bsky.feed.repost"])
    df["subject_cid"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y.get("cid")))
    df["subject_uri"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y.get("uri")))
    df["subject"] = df["subject"].apply(lambda x: apply_if_type(x, lambda y: y["uri"].split("/")[2]))
    df = df.loc[:,["cid_entry","did", "subject", "subject_cid", "subject_uri", "createdAt"]]
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    return df

def blocks(x):
    df = pd.DataFrame(x["app.bsky.graph.block"])
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)

    df = df.loc[:,["cid_entry","subject", "did", "createdAt"]]
    return df

def listblock(x):
    df = pd.DataFrame(x["app.bsky.graph.listblock"])
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    return df.loc[:,["cid_entry","subject","createdAt","did"]]

def create_list(data):
    df = pd.DataFrame(data["app.bsky.graph.list"])
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    df = df.loc[:,["cid_entry","purpose","description","did","createdAt"]]
    return df

def listitem(x):
    df = pd.DataFrame(x["app.bsky.graph.listitem"])
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    df = df.loc[:,["cid_entry","list","subject","createdAt","did"]]
    return df


def get_quote_info(x):
    if isinstance(x, dict):
        if x.get("$type") == "app.bsky.embed.record":
            return x.get("record", {}).get("cid", None), x.get("record", {}).get("uri", None)
        elif x.get("$type") == "app.bsky.embed.recordWithMedia":
            return x.get("record", {}).get("record", {}).get("cid", None), x.get("record", {}).get("record", {}).get("uri", None)
    return None, None

def threadgate(x):
    df = pd.DataFrame(x["app.bsky.feed.threadgate"])
    df["rule_type"] = df["allow"].apply(lambda x: apply_if_type(x, lambda y: y[0].get("$type") if isinstance(y, list) and len(y) > 0 else None))
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    df = df.loc[:,["cid_entry","post","rule_type", "createdAt", "did"]]
    return df

def posts(x):
    df = pd.DataFrame(x["app.bsky.feed.post"])
    #Error uploading data to posts: A string literal cannot contain NUL (0x00) characters.
    df["text"] = df["text"].apply(lambda x: x.replace("\x00", "") if isinstance(x, str) else x)
    
    df["has_image"] = df["embed"].apply(lambda x: apply_if_type(x, lambda y: True if isinstance(y, dict) and (y["$type"] == "app.bsky.embed.images" or y["$type"] == "app.bsky.embed.recordWithMedia") else False))
    df["has_image"] = df["has_image"].astype(bool)

    df["link"] = df.embed.apply(lambda x: apply_if_type(x, lambda y: y.get("external", {}).get("uri") if isinstance(y, dict) and y.get("$type") == "app.bsky.embed.external" else None))
    
    df["quote_cid"], df["quote_uri"] = zip(*df.embed.apply(get_quote_info))
    
    df["quote_did"] = df["quote_uri"].apply(lambda x: apply_if_type(x, lambda y: y.split("/")[2] if isinstance(y, str) and len(y.split("/")) > 2 else None, default=None))
    
    df["reply_root_cid"] = df["reply"].apply(lambda x: apply_if_type(x, lambda y: y.get("root", {}).get("cid")))
    
    df["reply_root_uri"] = df["reply"].apply(lambda x: apply_if_type(x, lambda y: y.get("root", {}).get("uri")))
    
    df["reply_root_did"] = df["reply_root_uri"].apply(lambda x: apply_if_type(x, lambda y: y.split("/")[2] if isinstance(y, str) and len(y.split("/")) > 2 else None, default=None))
    
    df["reply_parent_cid"] = df["reply"].apply(lambda x: apply_if_type(x, lambda y: y.get("parent", {}).get("cid")))
    
    df["reply_parent_uri"] = df["reply"].apply(lambda x: apply_if_type(x, lambda y: y.get("parent", {}).get("uri")))
    
    df["reply_parent_did"] = df["reply_parent_uri"].apply(lambda x: apply_if_type(x, lambda y: y.split("/")[2] if isinstance(y, str) and len(y.split("/")) > 2 else None, default=None))
    
    df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True, errors='coerce')
    df['createdAt'] = df['createdAt'].where(df['createdAt'].notnull(), None)
    df['createdAt'] = df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(x) else None)
    df["langs"] = df["langs"].apply(lambda x: apply_if_type(x, lambda y: ", ".join(y) if isinstance(y, list) else None))
    
    df = df.loc[:, ["cid_entry", "text", "langs", "createdAt", "did", "has_image", "link", "quote_cid", "quote_uri", "quote_did", "reply_root_cid", "reply_root_uri", "reply_root_did", "reply_parent_cid", "reply_parent_uri", "reply_parent_did"]]
    return df
    


queries = [
    """CREATE TABLE IF NOT EXISTS profiles (
        cid_entry text,
        did text,
        description text,
        displayName text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS follows (
        cid_entry text,
        subject text,
        did text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS likes (
        cid_entry text,
        did text,
        subject text,
        subject_cid text,
        subject_uri text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS reposts (
        cid_entry text,
        did text,
        subject text,
        subject_cid text,
        subject_uri text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS blocks (
        cid_entry text,
        subject text,
        did text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS listblock (
        cid_entry text,
        subject text,
        createdAt timestamp,
        did text
    );""",
    
    """CREATE TABLE IF NOT EXISTS create_list (
        cid_entry text,
        purpose text,
        description text,
        did text,
        createdAt timestamp
    );""",
    
    """CREATE TABLE IF NOT EXISTS listitem (
        cid_entry text,
        list text,
        subject text,
        createdAt timestamp,
        did text
    );""",
    
    """CREATE TABLE IF NOT EXISTS threadgate (
        cid_entry text,
        post text,
        rule_type text,
        createdAt timestamp,
        did text
    );""",
    
    """CREATE TABLE IF NOT EXISTS feed_generator (
        cid_entry text,
        did text,
        createdAt timestamp,
        description text,
        displayName text,
        skyfeedBuilder_id_input text,
        skyfeedBuilder_id_regex text,
        skyfeedBuilder_id_remove text,
        skyfeedBuilder_id_sort text,
        skyfeedBuilder_value_regex text,
        skyfeedBuilder_sort_type text
    );""",
    
    """CREATE TABLE IF NOT EXISTS posts (
        cid_entry text,
        text text,
        langs text,
        createdAt timestamp,
        did text,
        has_image boolean,
        link text,
        quote_cid text,
        quote_uri text,
        quote_did text,
        reply_root_cid text,
        reply_root_uri text,
        reply_root_did text,
        reply_parent_cid text,
        reply_parent_uri text,
        reply_parent_did text
    );"""
]

def upload_data_to_table(data, table_name, conn):
    # Define the expected data types for each column based on the table schema
    expected_dtypes = {
        "profiles": {"cid_entry":str, "did": str, "description": str, "displayName": str, "createdAt": str},
        "follows": {"cid_entry":str, "subject": str, "did": str, "createdAt": str},
        "likes": {"cid_entry":str, "did": str, "subject": str, "subject_cid": str, "subject_uri": str, "createdAt": str},
        "reposts": {"cid_entry":str, "did": str, "subject": str, "subject_cid": str, "subject_uri": str, "createdAt": str},
        "blocks": {"cid_entry":str, "subject": str, "did": str, "createdAt": str},
        "listblock": {"cid_entry":str, "subject": str, "createdAt": str, "did": str},
        "create_list": {"cid_entry":str, "purpose": str, "description": str, "did": str, "createdAt": str},
        "listitem": {"cid_entry":str, "list": str, "subject": str, "createdAt": str, "did": str},
        "threadgate": {"cid_entry":str, "post": str, "rule_type": str, "createdAt": str, "did": str},
        "feed_generator": {"cid_entry":str, "did": str, "createdAt": str, "description": str, "displayName": str,
                        "skyfeedBuilder_id_input": str, "skyfeedBuilder_id_regex": str,
                        "skyfeedBuilder_id_remove": str, "skyfeedBuilder_id_sort": str,
                        "skyfeedBuilder_value_regex": str, "skyfeedBuilder_sort_type": str},
        "posts": {"cid_entry":str, "text": str, "langs": str, "createdAt": str, "did": str, "has_image": bool,
                "link": str, "quote_cid": str, "quote_uri": str, "quote_did": str, "reply_root_cid": str,
                "reply_root_uri": str, "reply_root_did": str, "reply_parent_cid": str, "reply_parent_uri": str,
                "reply_parent_did": str}
    }

    # Get the expected data types for the current table
    table_dtypes = expected_dtypes[table_name]

    # Check if the DataFrame has the expected columns
    missing_columns = set(table_dtypes.keys()) - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in DataFrame for table '{table_name}': {missing_columns}")
    
    # Convert data types of the DataFrame columns to match the expected types
    for column, dtype in table_dtypes.items():
        data[column] = data[column].astype(dtype)
    
    data.replace({'None': None}, inplace=True)

    # Upload the filtered data to the SQL table
    columns = data.columns.tolist()
    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
    values = [tuple(row) for _, row in data.iterrows()]
    with conn.cursor() as cursor:
        try:
            cursor.executemany(query, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error uploading data to {table_name}: {str(e)}")


def upload_data_to_sql(conn, data, verbose=True):
    c = conn.cursor()

    # Execute the queries to create the tables if they don't exist
    for query in queries:
        c.execute(query)
    conn.commit()

    # Define a lambda function to convert 'NaT' to None
    convert_nat_to_none = lambda df: df.apply(lambda x: x.map(lambda y: None if pd.isnull(y) else (None if pd.isnull(y).all() else y) if hasattr(y, 'all') else y))


    # List of data preparation functions
    data_preparation_functions = [
        posts, profiles, follows, likes, reposts, blocks, listblock,
        create_list, listitem, threadgate, feed_generator
    ]

    # Table names corresponding to each data preparation function
    table_names = [
        "posts", "profiles", "follows", "likes", "reposts", "blocks", "listblock",
        "create_list", "listitem", "threadgate", "feed_generator"
    ]

    # Iterate over the preparation functions and table names together
    for prep_func, table_name in zip(data_preparation_functions, table_names):
        try:
            df = prep_func(data)
            upload_data_to_table(df, table_name, conn)
        except Exception as e:
            print(f"Error uploading data to {table_name}: {str(e)}") # We dont check whether a key is present.

    if verbose:
        print("Data uploaded to SQL")
    conn.commit()

def create_data_dict(file):
    print(f"Processing {file}")
    path = f"../../get_repo_temp/Data/full_download/{file}"
    try:
        with gzip.open(path , "rt") as f:
            data = json.load(f) 
        sorted_entries = {}
        for user in data:
            if not data[user]:
                continue
            for entry in data[user]:
                if not isinstance(entry["$type"], str):
                    continue
                if entry["$type"] not in sorted_entries:
                    sorted_entries[entry["$type"]] = []
                save_entry = entry
                # remove $type
                save_entry["did"] = user
                sorted_entries[entry["$type"]].append(save_entry)
    except Exception as e:
        print("#"*50)
        print(f"\033[91mError reading or Processing file {file}: {str(e)}\033[0m")
        print("#"*50)
        return None
    return sorted_entries

def process_and_upload_file(file):
    data = create_data_dict(file)
    if data is None:
        return
    # Connect to the database
    conn = psycopg2.connect(
        dbname="xxx",
        user="xxx",
        password="xxx",
        host="xxx"
    )

    # Upload the data to the database
    upload_data_to_sql(conn, data, verbose=False)
    conn.close()

    # Append the processed file to the text file
    with open("../processed_files.txt", "a") as f:
        f.write(f"{file}\n")

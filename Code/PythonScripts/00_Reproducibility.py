import sqlite3
import pandas as pd
import time

conn = sqlite3.connect("../../../Descriptive/Data/bluesky_backfill.db")
cursor = conn.cursor()

tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
tables = [table[0] for table in tables]

dids = {}
for table in tables:
    if 'did' in [col[1] for col in cursor.execute(f"PRAGMA table_info({table})")]:
        dids[table] = cursor.execute(f"SELECT DISTINCT did FROM {table}").fetchall()
    else:
        print(f"No 'did' in {table}, skipping")

conn.close()

# Directly write DIDs to a plain text file
all_dids = set(x[0] for table in dids.values() for x in table)
with open("DIDs.txt", "w") as f:
    for did in all_dids:
        f.write(f"{did}\n")

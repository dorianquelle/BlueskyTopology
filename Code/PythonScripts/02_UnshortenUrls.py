import pandas as pd
import requests
from urllib.parse import urlparse
from collections import Counter
from tqdm import tqdm
import random
import psycopg2
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_fixed
import os
import json

# Connection setup
conn = psycopg2.connect(
    dbname="bluesky_backfill_new",
    user="dorianquelle",
    host="localhost"
)

# Define the query
query = "SELECT cid_entry, link FROM posts WHERE link != '' AND link IS NOT NULL ORDER BY link"
df = pd.read_sql_query(query, conn)

def extract_domain(url):
    try:
        return urlparse(url).netloc
    except Exception as e:
        return ''

def clean_youtube_url(url):
    url = url.split("?")[0]
    url = url.replace("youtu.be/", "youtube.com/watch?v=")
    return url

@retry(stop=stop_after_attempt(2), wait=wait_fixed(0.5))
def unshorten_url(url):
    if "youtu.be" in url:
        return clean_youtube_url(url)
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        return response.url
    except Exception as e:
        return url

# Extract domains
print("Extracting domains...")
df['domain'] = df['link'].apply(extract_domain)

# Load link shorteners
potential_link_shorteners = [
    'bit.ly', 'ow.ly', 't.co', 'goo.gl', 'tinyurl.com', 'buff.ly', 'dlvr.it',
    'wp.me', 'htn.to', 'flic.kr', 'amzn.to', 'amzn.asia', 'amzn.eu',
    'shorturl.at', 'a.co', 'pca.st', 'g.co', 'youtu.be'
]

link = "https://raw.githubusercontent.com/sambokai/ShortURL-Services-List/master/shorturl-services-list.csv"
shorteners_git = pd.read_csv(link)
short_urls = list(shorteners_git["domain name"].values)
link_shorteners = set(potential_link_shorteners + short_urls)

def process_row(row):
    if row['domain'] in link_shorteners:
        unshortened = unshorten_url(row['link'])
        return unshortened, unshortened != row['link']
    return row['link'], False

def process_batch(batch_df, batch_num):
    print(f"Processing batch {batch_num}...")
    log = {
        "total_urls": len(batch_df),
        "successful_unshortens": 0,
        "failed_unshortens": 0,
        "no_unshorten_needed": 0
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        results = list(tqdm(executor.map(process_row, [row for _, row in batch_df.iterrows()]), total=len(batch_df)))
    
    unshortened_urls, was_shortened = zip(*results)
    batch_df['unshortened_url'] = unshortened_urls
    
    log["successful_unshortens"] = sum(was_shortened)
    log["failed_unshortens"] = sum((batch_df['domain'].isin(link_shorteners)) & (batch_df['link'] == batch_df['unshortened_url']))
    log["no_unshorten_needed"] = log["total_urls"] - log["successful_unshortens"] - log["failed_unshortens"]

    # Export the batch
    os.makedirs(f"../UnshortenBatches/Batch_{batch_num}", exist_ok=True)
    batch_df.to_csv(f"../UnshortenBatches/Batch_{batch_num}/unshortened_urls.csv", index=False)
    
    # Write log file
    with open(f"../UnshortenBatches/Batch_{batch_num}/log_batch_{batch_num}.json", 'w') as f:
        json.dump(log, f, indent=4)

    return batch_df[['unshortened_url', 'cid_entry']].values.tolist(), log

# Process in batches of 1000
batch_size = 100_000
num_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)

for i in range(num_batches):
    batch_path = f"../UnshortenBatches/Batch_{i}"
    if os.path.exists(batch_path):
        print(f"Batch {i} already exists. Skipping...")
        continue
    
    start_idx = i * batch_size
    end_idx = min((i + 1) * batch_size, len(df))
    batch_df = df.iloc[start_idx:end_idx].copy()
    
    print(f"Processing batch {i} of {num_batches}... ({i / num_batches:.2%})")
    update_data, batch_log = process_batch(batch_df, i)

dfs = []
for batch in tqdm(os.listdir("../UnshortenBatches")):
    if not os.path.isdir(f"../UnshortenBatches/{batch}"):
        continue
    df = pd.read_csv(f"../UnshortenBatches/{batch}/unshortened_urls.csv")
    dfs.append(df)

df = pd.concat(dfs)

update_data = df[['unshortened_url', 'cid_entry']].values.tolist()

with conn.cursor() as cur:
        for unshortened_url, cid_entry in update_data:
            cur.execute("UPDATE posts SET unshortened_link = %s WHERE cid_entry = %s", (unshortened_url, cid_entry))
conn.commit()
conn.close()



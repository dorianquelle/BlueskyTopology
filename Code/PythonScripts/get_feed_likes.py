from requests.exceptions import RequestException
from datetime import datetime
from atproto.exceptions import BadRequestError
from atproto import Client
import os 
import json
from tqdm import tqdm 
from tenacity import retry, wait_fixed, stop_after_attempt
import time

client = Client()
BLUESKY_HANDLE = "dorianquelle.bsky.social"
BLUESKY_APP_PASSWORD = "mwho-b5nl-apd4-gypj"
client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def collect_likes(client, uri, cursor=None, likes=None):
    cursor = None
    old_cursor = None
    if likes is None:
        likes = []
    while True:
        try:
            fetched = client.get_likes(uri, cursor=cursor, limit=100)
            likes = likes + fetched.likes

        except RequestException as e:
            print(e)
            cursor = old_cursor
            continue
        except BadRequestError:
            return []
        except Exception as e:
            print(f"{datetime.datetime.now()} {e}")
            cursor = old_cursor
            continue
        
        if not fetched.cursor:
            break
        old_cursor = cursor
        cursor = fetched.cursor
    return likes

def get_info(x):
    return x.actor.did, x.actor.handle, x.actor.description, x.created_at

# Load downloaded feeds:
feeds_files = os.listdir("../FeedData/full_feed_download")
feeds_files = [f for f in feeds_files if f.endswith(".json")]

for feed_file in feeds_files:  
    with open(f"../FeedData/full_feed_download/{feed_file}", "r") as f:
        feeds = json.load(f)
    for user in tqdm(feeds.keys()):
        # Check if user is already exported
        if os.path.exists(f"../FeedData/feed_likes/{user}.json"):
            print(f"Skipping {user}")
            continue
        else:
            print(f"Processing {user}")
        user_feeds = {}
        for feed in feeds[user]:
            uri = feed["uri"]
            likes = collect_likes(client, uri)
            likes = [get_info(x) for x in likes]
            user_feeds[uri] = likes
            time.sleep(0.1)
        with open(f"../FeedData/feed_likes/{user}.json", "w") as f:
            json.dump(user_feeds, f, indent=2)
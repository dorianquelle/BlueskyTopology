"""
BlueSky Repository Downloader
============================

This script downloads and processes BlueSky repositories using DIDs (Decentralized Identifiers).
It supports multi-processing and multi-threading for efficient data collection.

Prerequisites:
-------------
1. A text file named 'unique_dids.txt' in the parent directory containing one DID per line
2. Required Python packages: pandas, requests, atproto, tqdm, tenacity

Directory Structure:
------------------
- Data/
    - full_download/   # Stores downloaded repository data
    - DID_REV/        # Stores DID revision information
- unique_dids.txt     # Input file with DIDs

Usage:
------
1. Process all DIDs:
   ```
   python download_repos_multip.py
   ```
   or explicitly:
   ```
   python download_repos_multip.py --mode all
   ```

2. Process specific DID file:
   ```
   python download_repos_multip.py --mode process --input-file your_dids.txt
   ```

Optional Arguments:
-----------------
--processes: Number of processes to use (default: CPU count)
--mode: Operation mode ['all', 'process'] (default: 'all')
--input-file: Input file containing DIDs to process (required for 'process' mode)

Output:
-------
1. Compressed JSON files in Data/full_download/
   Format: chunk_[id]_[timestamp]_[uuid].json.gz

2. Revision files in Data/DID_REV/
   Format: rev_[chunk_id]_[uuid].txt
   Content: did,revision,timestamp

Performance:
-----------
- Uses CHUNK_SIZE of 100,000 DIDs per chunk
- Employs THREADS_PER_PROCESS (10) threads per process
- Automatically skips previously processed DIDs
"""

import pandas as pd
import concurrent.futures
import requests
from atproto import CAR, Client
from tqdm import tqdm
import os
import json
import gzip
from datetime import datetime
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
import random
import argparse
from requests.exceptions import HTTPError
import multiprocessing
import threading
import logging
from pathlib import Path
import uuid
import queue

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CHUNK_SIZE = 100_000
THREADS_PER_PROCESS = 10

def load_all_dids():
    with open("../../unique_dids.txt", "r") as f:
        return [line.strip() for line in f]

def load_processed_dids():
    processed_dids = set()
    rev_files = Path("../../Data/DID_REV").glob("rev_*.txt")
    for file in rev_files:
        with open(file, "r") as f:
            processed_dids.update(line.split(',')[0] for line in f)
    return processed_dids

@retry(wait=wait_fixed(3), stop=stop_after_attempt(2))
def get_plc_info(did):
    url = f"https://plc.directory/{did}/"
    response = requests.get(url)
    return response.json()["service"][0]["serviceEndpoint"]

@retry(wait=wait_fixed(3), stop=stop_after_attempt(2))
def get_repo(did):
    pds = get_plc_info(did)
    response = requests.get(f"{pds}/xrpc/com.atproto.sync.getRepo",
                            headers={"Accept": "application/vnd.ipld.car"},
                            params={"did": did})
    
    if response.status_code != 200 or not response.content:
        logger.error(f"DID {did} skipped due to empty content or bad status. Status code: {response.status_code}")
        return None
    
    a = response.content

    if not a:
        logger.error(f"DID {did} skipped due to empty content.")
        return None
    
    try:
        car_file = CAR.from_bytes(a)
    except Exception as e:
        logger.error(f"DID {did} error; Occurred in function get_repo: {e}")
        return None
    
    cblocks = car_file.blocks
    content = []
    
    for block in cblocks:
        content_to_append = cblocks.get(block)
        if hasattr(block, "_cid"):
            content_to_append["cid_entry"] = str(block._cid)
        elif hasattr(block, "cid"):
            content_to_append["cid_entry"] = str(block.cid)
        else:
            content_to_append["cid_entry"] = "no cid found"

        content.append(content_to_append)
    
    rev_entry = [x for x in content if "rev" in x.keys()]
    rev = rev_entry[0].get('rev') if rev_entry else None
    content = [x for x in content if "$type" in x.keys() or "type" in x.keys()]
    return content, rev

def convert_bytes_to_str(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    if isinstance(obj, list):
        return [convert_bytes_to_str(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_bytes_to_str(key): convert_bytes_to_str(value) for key, value in obj.items()}
    return obj

def process_did(did):
    try:
        result, rev = get_repo(did)
        if result is not None:
            result = convert_bytes_to_str(result)
            return did, result, rev
    except Exception as e:
        logger.error(f"Error processing DID {did}: {e}")
    return None

def worker(did_queue, results_queue, revs_queue, progress_bar):
    while True:
        did = did_queue.get()
        if did is None:
            break
        result = process_did(did)
        if result:
            did, data, rev = result
            results_queue.put((did, data))
            revs_queue.put((did, rev))
        progress_bar.update(1)
        did_queue.task_done()

def process_chunk(process_id, chunk_id, dids):
    start_time = datetime.now()
    logger.info(f"Process {process_id}: Starting chunk {chunk_id} at {start_time}")

    did_queue = queue.Queue()
    results_queue = queue.Queue()
    revs_queue = queue.Queue()

    for did in dids:
        did_queue.put(did)

    with tqdm(total=len(dids), desc=f"Process {process_id}, Chunk {chunk_id}", unit="DID") as progress_bar:
        threads = []
        for _ in range(THREADS_PER_PROCESS):
            t = threading.Thread(target=worker, args=(did_queue, results_queue, revs_queue, progress_bar))
            t.start()
            threads.append(t)

        # Wait for all DIDs to be processed
        did_queue.join()

        # Stop the workers
        for _ in range(THREADS_PER_PROCESS):
            did_queue.put(None)
        for t in threads:
            t.join()

    # Collect results
    results = {}
    revs = {}
    while not results_queue.empty():
        did, data = results_queue.get()
        results[did] = data
    while not revs_queue.empty():
        did, rev = revs_queue.get()
        revs[did] = rev

    # Flush results to disk
    flush_results_to_disk(results, chunk_id)
    flush_revs_to_disk(revs, chunk_id)

    end_time = datetime.now()
    logger.info(f"Process {process_id}: Finished chunk {chunk_id} at {end_time}. Duration: {end_time - start_time}")

    return set(results.keys())

def flush_results_to_disk(results, chunk_id):
    compressed_data = gzip.compress(json.dumps(results).encode('utf-8'))
    os.makedirs("../../Data/full_download", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_uuid = uuid.uuid4()
    file_name = f"../../Data/full_download/chunk_{chunk_id}_{timestamp}_{file_uuid}.json.gz"
    with open(file_name, 'wb') as f:
        f.write(compressed_data)

def flush_revs_to_disk(revs, chunk_id):
    os.makedirs("../../Data/DID_REV", exist_ok=True)
    file_uuid = uuid.uuid4()
    file_name = f"../../Data/DID_REV/rev_{chunk_id}_{file_uuid}.txt"
    with open(file_name, "a") as f:
        for did, rev in revs.items():
            f.write(f"{did},{rev},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

def split_remaining_dids(all_dids, processed_dids, split_ratio=0.7):
    remaining_dids = list(set(all_dids) - set(processed_dids))
    random.shuffle(remaining_dids)
    split_index = int(len(remaining_dids) * split_ratio)
    return remaining_dids[:split_index], remaining_dids[split_index:]

def save_dids_to_file(dids, filename):
    with open(filename, 'w') as f:
        for did in dids:
            f.write(f"{did}\n")

def main():
    parser = argparse.ArgumentParser(description='Process DIDs with multiprocessing and multithreading.')
    parser.add_argument('--processes', type=int, default=multiprocessing.cpu_count(), help='Number of processes to use')
    parser.add_argument('--mode', choices=['all', 'process'], default='all', help='Operation mode')
    parser.add_argument('--input-file', help='Input file containing DIDs to process')
    args = parser.parse_args()

    if args.mode == 'process':
        if not args.input_file:
            print("Error: --input-file is required when using --mode process")
            return
        with open(args.input_file, 'r') as f:
            dids_to_process = [line.strip() for line in f]
    else:
        all_dids = load_all_dids()
        processed_dids = load_processed_dids()
        dids_to_process = list(set(all_dids) - processed_dids)

    random.shuffle(dids_to_process)

    # Create chunks of 100,000 DIDs
    chunks = list(enumerate([dids_to_process[i:i + CHUNK_SIZE] for i in range(0, len(dids_to_process), CHUNK_SIZE)]))
    
    # Process chunks using multiprocessing
    with multiprocessing.Pool(args.processes) as pool:
        results = pool.starmap(process_chunk, [(i, chunk_id, chunk_dids) for i, (chunk_id, chunk_dids) in enumerate(chunks)])

    total_processed = sum(len(processed_dids) for processed_dids in results)
    logger.info(f"Total DIDs processed: {total_processed}")

if __name__ == "__main__":
    main()
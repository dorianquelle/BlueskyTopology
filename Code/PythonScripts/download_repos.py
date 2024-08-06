"""
This script processes IDs (DIDs) from the Bluesky social network, fetches their associated data, and stores it.

Main functions:

load_dids(chunk_number: int) -> List[str]:
    Loads DIDs from a text file for a given chunk number.
    Input: Chunk number (int)
    Output: List of DIDs (strings)

get_plc_info(did: str) -> str:
    Retrieves the PLC endpoint for a given DID.
    Input: DID (string)
    Output: PLC service endpoint (string)

get_repo(did: str) -> Optional[List[Dict]]:
    Fetches and processes the repository data for a given DID.
    Input: DID (string)
    Output: List of dictionaries containing repository data, or None if an error occurs

process_dids(dids: List[str], batch_number: int, chunk_number: int) -> None:
    Processes a batch of DIDs concurrently, fetching and storing their data.
    Input:
        dids: List of DIDs to process
        batch_number: Current batch number
        chunk_number: Current chunk number
    Output: None (data is processed and stored)

Helper functions:
    convert_bytes_to_str: Recursively converts bytes to strings in nested structures.
    flush_errors_to_disk, flush_results_to_disk, flush_revs_to_disk, flush_dids_to_disk:
        Functions to write accumulated data to disk.

The script uses concurrent processing to handle multiple DIDs simultaneously, implements
retry logic for network requests, and includes error handling and logging. It also
uses command-line arguments to specify which chunk of DIDs to process.
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

def load_dids(chunk_number):
    with open(f"../Data/dids_chunk_{chunk_number}.txt", "r") as f:
        dids = f.read().split("\n")
    return dids


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
        error_buffer.append(f"DID {did} skipped due to empty content or bad status. Status code: {response.status_code}\n")
        return None  
    
    a = response.content

    if not a: 
        error_buffer.append(f"DID {did} skipped due to empty content or bad status.\n")
        return None
    
    try:
        car_file = CAR.from_bytes(a)
    except BaseException as e:
        error_buffer.append(f"DID {did} error; CAUGHT PANIC WITH BASE EXCEPTION; Occurred in function get_repo: {e}\n")
        return
    except Exception as e:
        error_buffer.append(f"DID {did} error; Occurred in function get_repo: {e}\n")
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

        content.append(cblocks.get(block))
    
    rev_entry = [x for x in content if "rev" in x.keys()]
    rev_buffer.append(f"{did}, {rev_entry[0].get('rev')}, {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    content = [x for x in content if "$type" in x.keys() or "type" in x.keys()]
    return content

def convert_bytes_to_str(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    if isinstance(obj, list):
        return [convert_bytes_to_str(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_bytes_to_str(key): convert_bytes_to_str(value) for key, value in obj.items()}
    return obj

def flush_errors_to_disk():
    print(f"\033[91mFlushing errors to disk - THERE ARE {len(error_buffer)} ERRORS\033[0m")
    if error_buffer:
        with open(f"../Data/error_log_{chunk_number}.txt", "a") as error_log:
            error_log.write(''.join(error_buffer))
        error_buffer.clear()

def flush_results_to_disk():
    print(f"\033[91mFlushing results to disk - THERE ARE {len(results)} RESULTS\033[0m")
    if results:
        compressed_data = gzip.compress(json.dumps(results).encode('utf-8'))
        os.makedirs("../Data/full_download", exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"../Data/full_download/batch_{batch_number}_{chunk_number}_{timestamp}.json.gz"
        with open(file_name, 'wb') as f:
            f.write(compressed_data)
        results.clear()

def flush_revs_to_disk():
    print(f"\033[91mFlushing revs to disk - THERE ARE {len(rev_buffer)} REVS\033[0m")
    if rev_buffer:
        with open(f"../Data/rev_{chunk_number}.txt", "a") as rev_file:
            rev_file.write(''.join(rev_buffer))
        rev_buffer.clear()

def flush_dids_to_disk():
    print(f"\033[91mFlushing DIDs to disk - THERE ARE {len(dids_buffer)} DIDs\033[0m")
    if dids_buffer:
        with open(f"../Data/download_log_{chunk_number}.txt", "a") as log_file:
            log_file.write(''.join(dids_buffer))
        dids_buffer.clear()

def process_dids(dids, batch_number, chunk_number):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_repo, did): did for did in dids}
        error_count = 0
        
        with tqdm(total=len(dids), desc="Processing DIDs", unit="DID") as pbar:
            for future in concurrent.futures.as_completed(futures):
                did = futures[future]
                try:
                    result = future.result()
                    result = convert_bytes_to_str(result)
                    results[did] = result
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    dids_buffer.append(f"{did}, {timestamp}\n")
                except BaseException as e:
                    error_buffer.append(f"DID {did} error; CAUGHT PANIC WITH BASE EXCEPTION; Occurred in function process_dids: {e}\n")
                except Exception as e:
                    error_count += 1
                    error_percentage = (error_count / len(dids)) * 100
                    pbar.set_description(f"Batch: {batch_number}. Processing DIDs (\033[91mErrors: {error_count}, {error_percentage:.2f}%\033[0m)")
                    error_buffer.append(f"DID {did} error: {e}\n")
                pbar.update(1)
        
        print(f"Batch {batch_number} completed - Starting to flush to disk")
        flush_errors_to_disk()
        flush_results_to_disk()
        flush_revs_to_disk()
        flush_dids_to_disk()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process DIDs.')
    parser.add_argument('--chunk', type=int, required=True, help='Chunk number to process')
    args = parser.parse_args()
    chunk_number = args.chunk
    
    dids = load_dids(chunk_number)
    prev_len = len(dids)

    download_log_path = f"../Data/download_log_{chunk_number}.txt"
    if not os.path.exists(download_log_path):
        open(download_log_path, 'a').close()  

    with open(download_log_path, "r") as log_file:
        processed_dids = [line.split(",")[0].strip() for line in log_file.readlines()]
    
    processed_dids = set(processed_dids)
    dids = set(dids) - processed_dids
    dids = list(dids)
    
    random.shuffle(dids)
    print(f"Filtered out {prev_len - len(dids)} DIDs that have already been processed. {100 * (prev_len - len(dids)) / prev_len:.2f}%")
    
    batch_size = 10_000
    num_batches = (len(dids) + batch_size - 1) // batch_size
    
    for batch_number in range(num_batches):
        start_index = batch_number * batch_size
        end_index = min((batch_number + 1) * batch_size, len(dids))
        batch_dids = dids[start_index:end_index]

        # Declare variables for each batch inside the loop
        error_buffer = []
        results = {}
        rev_buffer = []
        dids_buffer = []

        process_dids(batch_dids, batch_number + 1, chunk_number)

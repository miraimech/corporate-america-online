import json
import os
import logging
import hashlib

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PROCESSING_PATH = os.path.join(PROJECT_PATH, 'analysis', 'processing')
HASHES_PATH = os.path.join(PROJECT_PATH, 'analysis', 'hashes')
LOGS_PATH = os.path.join(PROJECT_PATH, 'logs')

if not os.path.exists(PROCESSING_PATH):
    os.makedirs(PROCESSING_PATH)
if not os.path.exists(HASHES_PATH):
    os.makedirs(HASHES_PATH)
if not os.path.exists(LOGS_PATH):
    os.makedirs(LOGS_PATH)

logging.basicConfig(filename=os.path.join(LOGS_PATH, 'truncation.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

processed_hashes = set()

def load_processed_hashes():
    hashes_file_path = os.path.join(HASHES_PATH, 'truncation.txt')
    if os.path.exists(hashes_file_path):
        with open(hashes_file_path, 'r') as file:
            global processed_hashes
            processed_hashes = set(file.read().splitlines())
    else:
        logging.info("No existing hashes file found. Starting fresh.")
        
load_processed_hashes()

def file_hash(data):
    return hashlib.sha256(data.encode()).hexdigest()

def update_hashes_file(hash_entry):
    with open(os.path.join(HASHES_PATH, 'truncation.txt'), 'a') as hashes_file:
        hashes_file.write(hash_entry + "\n")

def read_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def write_truncated(base_filename, truncated_number, truncated_data):
    file_content = json.dumps(truncated_data, indent=4)
    data_hash = file_hash(file_content)
    hash_entry = f"{base_filename}_{truncated_number}_Truncated = {data_hash}"

    if data_hash not in processed_hashes:
        new_file_path = os.path.join(PROCESSING_PATH, f"{base_filename}_{truncated_number}_Truncated.txt")
        with open(new_file_path, 'w') as file:
            file.write(file_content)
        update_hashes_file(data_hash)
        processed_hashes.add(data_hash)
        logging.info(f"Processed and created: {new_file_path}")
    else:
        logging.info(f"Duplicate data found for {base_filename}, skipping.")

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def process_file(file_path, filename):
    base_filename = filename.replace('_Data.txt', '')
    json_data = read_json(file_path)

    flattened_data = flatten_json(json_data)
    keys = sorted(flattened_data.keys())

    current_truncated = []
    truncated_number = 1

    for key in keys:
        current_truncated.append({key: flattened_data[key]})

        if len(json.dumps(current_truncated, indent=4)) > 511:
            write_truncated(base_filename, truncated_number, current_truncated[:-1])
            current_truncated = [{key: flattened_data[key]}]
            truncated_number += 1

    if current_truncated:
        write_truncated(base_filename, truncated_number, current_truncated)

    os.remove(file_path)
    logging.info(f"Deleted original file: {filename}")

def process_files():
    logging.info("Beginning processing of files.")
    for filename in os.listdir(PROCESSING_PATH):
        if filename.endswith('_Data.txt'):
            file_path = os.path.join(PROCESSING_PATH, filename)
            process_file(file_path, filename)

process_files()

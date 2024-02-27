import json
import requests
import logging
import os
import re
import hashlib

LOCAL_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
PROCESSING_PATH = os.path.join(LOCAL_PATH, 'analysis', 'processing')
HASH_PATH = os.path.join(LOCAL_PATH, 'analysis', 'hashes')
LOG_PATH = os.path.join(LOCAL_PATH, 'analysis', 'logs')

if not os.path.exists(PROCESSING_PATH):
    os.makedirs(PROCESSING_PATH)
if not os.path.exists(HASH_PATH):
    os.makedirs(HASH_PATH)
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

logging.basicConfig(filename=os.path.join(LOG_PATH, 'downloader.log'), level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def is_empty(data):
    if isinstance(data, dict):
        return all(is_empty(v) for v in data.values())
    if isinstance(data, list):
        return all(is_empty(v) for v in data)
    return not data

def save_data(json_data, base_file_name):
    file_path = os.path.join(PROCESSING_PATH, base_file_name)
    with open(file_path, 'w') as file:
        json.dump(json_data, file, indent=4)
    logging.info(f"Data saved to {file_path}")

def download_json(api_url, base_file_name):
    response = requests.get(api_url)
    if response.status_code != 200:
        logging.error(f"Failed to download data from {api_url}")
    return response.text

def clean_json(cleaned_json, recursion_depth=0, max_recursion_depth=10):
    cleaned_json = cleaned_json.replace('\n', '').replace('\r', '').replace('\t', '')

    patterns = [
        (r'": \* ', r'": null '),
        (r"'(\w+)'\s*:", r'"\1":'),
        (r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3'),
        (r'(:\s*)(\w+)(\s*[,\]})])', r'\1"\2"\3'),
        (r'(\}\s*)(\{)', r'\1, \2'),
        (r'(\]\s*)(\[)', r'\1, \2'),
        (r'(,\s*)([\]}])', r'\2'),
        (r'truee|falsee|nul', lambda m: m.group(0)[0:-1]),
        (r'(?<!\\)"(.*?)(?<!\\)"', lambda m: '"' + m.group(1).replace('"', '\\"') + '"'), 
    ]

    for pattern, replacement in patterns:
        cleaned_json = re.sub(pattern, replacement, cleaned_json)
        try:
            json.loads(cleaned_json)
        except json.JSONDecodeError:
            continue
        else:
            break

    return cleaned_json

def create_hash(data):
    return hashlib.sha256(data.encode()).hexdigest()

def save_hash(data, file_name):
    sha256_hash = create_hash(data)
    hash_file_path = os.path.join(HASH_PATH, 'downloader.txt')
    
    existing_hashes = load_hashes()

    if file_name not in existing_hashes or existing_hashes[file_name] != sha256_hash:
        with open(hash_file_path, 'a') as hash_file:
            hash_file.write(f"{file_name}='{sha256_hash}'\n")

def load_hashes():
    hashes = {}
    hash_file_path = os.path.join(HASH_PATH, 'downloader.txt')
    if os.path.exists(hash_file_path):
        with open(hash_file_path) as hash_file:
            for line in hash_file:
                name, hash_value = line.split('=')
                hashes[name.strip()] = hash_value.strip().strip("'")
    return hashes

def main():
    existing_hashes = load_hashes()
    new_data_processed = False
    urls = [
        ("https://markets.newyorkfed.org/api/ambs/all/announcements/summary/latest.json", "AMBS_Announcements_Data.txt"),
        ("https://markets.newyorkfed.org/api/ambs/all/results/summary/latest.json", "AMBS_Results_Data.txt"),
        ("https://markets.newyorkfed.org/api/fxs/all/latest.json", "FX_Swaps_Announcements_Data.txt"),
        ("https://markets.newyorkfed.org/api/ambs/all/results/summary/latest.json", "FX_Swaps_Results_Data.txt"),
        ("https://markets.newyorkfed.org/api/marketshare/qtrly/latest.json", "Market_Share_Quarterly_Data.txt"),
        ("https://markets.newyorkfed.org/api/marketshare/ytd/latest.json", "Market_Share_Yearly_Data.txt"),
        ("https://markets.newyorkfed.org/api/rates/all/latest.json", "Rates_Data.txt"),
        ("https://markets.newyorkfed.org/api/rp/all/all/results/latest.json", "Repo_Results_Data.txt"),
        ("https://markets.newyorkfed.org/api/rp/all/all/announcements/latest.json", "Repo_Announcements_Data.txt"),
        ("https://markets.newyorkfed.org/api/seclending/all/results/summary/latest.json", "Securities_Lending_Data.txt"),
        ("https://markets.newyorkfed.org/api/tsy/all/announcements/summary/latest.json", "Treasury_Securities_Announcements_Data.txt"),
        ("https://markets.newyorkfed.org/api/tsy/all/results/summary/latest.json", "Treasury_Securities_Results_Data.txt"),
        ("https://markets.newyorkfed.org/api/tsy/all/operations/summary/latest.json", "Treasury_Securities_Operations_Data.txt"),
        ("https://www.federalregister.gov/api/v1/public-inspection-documents/current.json", "Public_Inspection_Data.txt"),
        ("https://api.stlouisfed.org/fred/releases?api_key=6f19389f3e13501941e023c61848ab90&file_type=json", "FRED_Data.txt"),
    ]

    for api_url, file_name in urls:
        try:
            json_text = download_json(api_url, file_name)
            cleaned_json = clean_json(json_text)
            json_data = json.loads(cleaned_json)
            new_hash = create_hash(cleaned_json)
            if not is_empty(json_data) and (file_name not in existing_hashes or existing_hashes[file_name] != new_hash):
                save_data(json_data, file_name)
                save_hash(cleaned_json, file_name)
                existing_hashes[file_name] = new_hash
                new_data_processed = True
        except Exception as e:
            logging.error(f"Error processing {api_url}: {e}")

    if not new_data_processed:
        logging.info("No new data to process.")

if __name__ == "__main__":
    main()
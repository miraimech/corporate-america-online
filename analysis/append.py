import os
import logging
import re
import datetime
import json
from module import DataObject

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PROCESSING_PATH = os.path.join(PROJECT_PATH,'analysis', 'processing')
DATA_PATH = os.path.join(PROJECT_PATH, 'analysis', 'data')
LOG_PATH = os.path.join(PROJECT_PATH, 'analysis', 'logs')

if not os.path.exists(PROCESSING_PATH):
    os.makedirs(PROCESSING_PATH)
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

logging.basicConfig(filename=os.path.join(LOG_PATH, 'append.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_url(content):
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    match = url_pattern.search(content)
    return match.group(0) if match else None

def extract_date(content):
    date_patterns = [
        (r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b', "%m-%d-%Y"),
        (r'\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b', "%Y-%m-%d"),
        (r'\b(\w+) (\d{1,2}), (\d{4})\b', "%B %d, %Y"),
        (r'\b(\d{1,2}) (\w+), (\d{4})\b', "%d %B, %Y")
    ]
    for pattern, date_format in date_patterns:
        match = re.search(pattern, content)
        if match:
            return datetime.datetime.strptime(match.group(), date_format).strftime('%B %d, %Y')
    return None

def process_truncated_files():
    for filename in os.listdir(PROCESSING_PATH):
        if filename.endswith('_Truncated.txt'):
            base_name, _ = os.path.splitext(filename)[0].rsplit('_', 1)
            file_path = os.path.join(PROCESSING_PATH, filename)
            with open(file_path, 'r') as f:
                content = f.read()

            url = extract_url(content)
            date = extract_date(content)

            json_file_path = os.path.join(DATA_PATH, f"{base_name}.json")
            if os.path.isfile(json_file_path):
                with open(json_file_path, 'r+') as f:
                    data = json.load(f)
                    data_obj = DataObject(**data)
            else:
                data_obj = DataObject(base_name=base_name)

            data_obj.url = url if url else data_obj.url
            data_obj.date = date if date else data_obj.date

            with open(json_file_path, 'w') as f:
                f.write(json.dumps(data_obj.__dict__, indent=4))

            logging.info(f"Processed {filename} and updated {json_file_path}")

def update_data_objects_with_copyright(data_objects):
    copyright_texts = {
        "AMBS_Announcements": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "AMBS_Results": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "FX_Swaps_Announcements": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "FX_Swaps_Results": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Market_Share_Quarterly": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Market_Share_Yearly": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Rates": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Repo_Results": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Repo_Announcements": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Securities_Lending": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Treasury_Securities_Announcements": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Treasury_Securities_Results": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Treasury_Securities_Operations": "© 2024 Federal Reserve Bank of New York. Content from the New York Fed subject to the Terms of Use at newyorkfed.org.",
        "Public_Inspection": "Federal Register",
        "FRED": "Federal Reserve Economic Data"
    }

    for obj in data_objects:
        for key, text in copyright_texts.items():
            if key in obj.base_name:
                obj.copyright = text
                break

def save_data_objects(data_objects):
    for obj in data_objects:
        file_path = os.path.join(DATA_PATH, f"{obj.base_name}.json")
        with open(file_path, 'w') as file:
            file.write(obj.to_json())

def main():
    logging.info("Starting processing of truncated files.")
    data_objects = []
    process_truncated_files()
    update_data_objects_with_copyright(data_objects)
    save_data_objects(data_objects)
    logging.info("Completed processing of files.")

if __name__ == '__main__':
    main()
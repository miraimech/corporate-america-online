import json
import os
import glob
import re
import logging

LOCAL_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PROCESSING_PATH = os.path.join(LOCAL_PATH, 'analysis', 'processing')
LOG_PATH = os.path.join(LOCAL_PATH, 'analysis', 'logs')

if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

logging.basicConfig(filename=os.path.join(LOG_PATH, 'preparation.log'), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_urls(text):
    return re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\'(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

def is_numerical(text):
    return bool(re.match(r'^[\d\.\-\/]+$', text))

def process_text(text):
    text = remove_urls(text).strip()
    sentences = [sentence.strip() + '.' for sentence in text.split('.') if sentence.strip() and len(sentence.strip()) > 1]
    if not sentences:
        return ''
    if len(sentences) > 1 and re.search(r'http[s]?://', sentences[1]):
        return sentences[0]
    return sentences[0] if len(sentences) == 1 else ' '.join(sentences)

def process_json_file(file_path):
    logging.debug(f"Processing file: {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
        
    relevant_texts = []
    for item in data:
        textual_values = [value for value in item.values() if isinstance(value, str)]
        for text in textual_values:
            processed_text = process_text(text)
            if len(processed_text) > 50:
                relevant_texts.append(processed_text)
                continue
            
            numerical_values = [text for text in textual_values if is_numerical(text)]
            if len(numerical_values) >= 3:
                relevant_texts.extend(numerical_values)

    if not relevant_texts:
        logging.debug(f"No relevant content for {file_path}. Skipping creation of a prepared file.")
        return

    base_name = os.path.basename(file_path).replace('_Truncated.txt', '_Prepared.txt')
    output_path = os.path.join(PROCESSING_PATH, base_name)
    
    with open(output_path, 'w') as file:
        for text in relevant_texts:
            file.write(text + '\n')
    logging.debug(f"Saved prepared data to: {output_path}")

def delete_truncated_files():
    for file_path in glob.glob(os.path.join(PROCESSING_PATH, '*_Truncated.txt')):
        logging.debug(f"Deleting file: {file_path}")
        os.remove(file_path)

def main():
    logging.info(f"Starting processing of truncated files in {PROCESSING_PATH}.")
    files_found = glob.glob(os.path.join(PROCESSING_PATH, '*_Truncated.txt'))
    if not files_found:
        logging.info(f"No files found in {PROCESSING_PATH} to process.")
        return
    for file_path in files_found:
        process_json_file(file_path)
    
    delete_truncated_files()
    logging.info("Completed processing of files.")

if __name__ == "__main__":
    main()

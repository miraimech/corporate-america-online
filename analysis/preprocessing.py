import re
import os
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string

# Download necessary NLTK data packages if not already present
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Setup paths
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PROCESSING_PATH = os.path.join(PROJECT_PATH, 'analysis', 'processing')
LOG_PATH = os.path.join(PROJECT_PATH, 'analysis', 'log')

# Ensure directories exist
if not os.path.exists(PROCESSING_PATH):
    os.makedirs(PROCESSING_PATH)
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

def is_valid_data(text, min_length=100):
    """Check if the text data is considered valid."""
    if len(text) < min_length or not re.search('[a-zA-Z]', text):
        return False
    return True

def preprocess_text(text):
    text = text.lower()
    text = re.sub(r'http[s]?://\S+', '', text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    words = word_tokenize(text)
    filtered_words = [word for word in words if word not in stopwords.words('english')]
    lemmatizer = WordNetLemmatizer()
    lemmatized_words = [lemmatizer.lemmatize(word) for word in filtered_words]
    return ' '.join(lemmatized_words)

def process_file(file_path, base_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()
    
    if not is_valid_data(text):
        print(f"Skipping invalid or incomplete data in file: {file_path}")
        return False
    
    preprocessed_text = preprocess_text(text)
    
    if len(preprocessed_text) > 0:
        output_path = os.path.join(PROCESSING_PATH, f"{base_name}_Preprocessed.txt")
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(preprocessed_text)
        print(f"Processed and saved: {output_path}")
    return True

def process_files():
    for filename in os.listdir(PROCESSING_PATH):
        if filename.endswith("_Prepared.txt"):
            base_name = filename.replace('_Prepared.txt', '')
            file_path = os.path.join(PROCESSING_PATH, filename)
            process_file(file_path, base_name)
    
    # After processing all files, delete all _Prepared.txt files
    for filename in os.listdir(PROCESSING_PATH):
        if filename.endswith("_Prepared.txt"):
            os.remove(os.path.join(PROCESSING_PATH, filename))
            print(f"Deleted original file: {filename}")

def main():
    process_files()

if __name__ == '__main__':
    main()

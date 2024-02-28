import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string

def preprocess_text(text):
    # Convert text to lowercase
    text = text.lower()
    # Remove URLs
    text = re.sub(r'http[s]?://\S+', '', text)
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # Remove numbers - optional, based on whether number information is crucial for your task
    text = re.sub(r'\d+', '', text)
    # Tokenization
    words = word_tokenize(text)
    # Remove stopwords
    filtered_words = [word for word in words if word not in stopwords.words('english')]
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_words = [lemmatizer.lemmatize(word) for word in filtered_words]
    # Reconstruct the text
    preprocessed_text = ' '.join(lemmatized_words)
    return preprocessed_text


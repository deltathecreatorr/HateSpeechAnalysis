import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

def extract_words(filepath: str, top_n: int = 100) -> list[str]:
    """
    Extracts the most important keywords from the cleaned text in the CSVs 
    
    Arguments:
        filepath -- the path to the CSV file containing the cleaned text
        top_n -- the number of top keywords to extract
    Returns:
        A list of the most important keywords
    """
    df = pd.read_csv(filepath)

    corpus = df['cleaned_text'].fillna('').tolist()
    vectoriser = TfidfVectorizer(stop_words='english', max_df=0.8, min_df=5)
    matrix = vectoriser.fit_transform(corpus)

    #scores = 
    
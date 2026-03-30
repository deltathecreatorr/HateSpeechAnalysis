from dataset import getRedditDataframe, getTwitterDataframe
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, TweetTokenizer

nltk.download('stopwords')
nltk.download('punkt')
stop_words = set(stopwords.words('english'))

tweet_tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True)

def clean(text: str, heavy=False, platform='reddit') -> str:
    """
    Removes only URLs and mentions so VADER can stil run reliably.
    
    Arguments:
        text: A string representing the text to be cleaned.
        platform: A string representing the platform ('reddit' or 'twitter').
    Returns:
        A string representing the cleaned text.
    """
    if not isinstance(text, str):
        return ""
    
    text = re.sub(r'http\S+|www\S+|https\S+|@\w+', '', text, flags=re.MULTILINE)
    if not heavy:
        return text.strip()
    else:
        if platform == 'twitter':
            tokens = tweet_tokenizer.tokenize(text)
        else:
            tokens = word_tokenize(text.lower())
        
        return ' '.join(w for w in tokens if w not in stop_words and w.isalpha())

def process_dataframe(df, platform='reddit') -> pd.DataFrame:
    """
    Function to handle Multi-platform structural and semantic cleaning.
    Arguments:
        df: A Pandas Dataframe containing the text data to be cleaned.
        platform: A string representing the platform ('reddit' or 'twitter').
    Returns:
        A Pandas Dataframe with the cleaned text.
    
    """
    cfg = {
        'twitter': (
            'text', 'username', 'created_at'
        ),
        'reddit': (
            'self_text', 'author_name', 'created_time'
        )
    }
    text_col, user_col, time_col = cfg[platform]

    df = df.dropna(subset=[text_col])
    df = df[df[text_col].str.strip() != '']
    df[user_col] = df[user_col].astype(str).str.strip().str.lower()

    if time_col:
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    
    df['vader_text'] = df[text_col].apply(clean, heavy=False, platform=platform)
    df['cleaned_text'] = df[text_col].apply(clean, heavy=True, platform=platform)

    return df[df['cleaned_text'].str.strip() != '']

    
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, TweetTokenizer

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('punkt_tab')

#Set of english stop words to be used for cleaning the text data
stop_words = set(stopwords.words('english'))

#A tokeniser specifically designed for tweets, which can handle twitter-specific language features like hashtags.
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
    
    #Preps the text for VADER by removing URLs and mentions, but keeping punctuation and stop words for better sentiment scoring.
    text = re.sub(r'http\S+|www\S+|https\S+|@\w+', '', text, flags=re.MULTILINE)
    if not heavy:
        return text.strip()
    else:
        #For TF-IDF and topic modeling, as they need stop words removed and punctuation removed.
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
    #Define column names based on the plotform, as the datasets differ
    cfg = {
        'twitter': (
            'text', 'username', 'created_at'
        ),
        'reddit': (
            'self_text', 'author_name', 'created_time'
        )
    }
    text_col, user_col, time_col = cfg[platform]

    #Drop rows with missing or empty text
    df = df.dropna(subset=[text_col])
    df = df[df[text_col].str.strip() != '']

    #Prevent the same user from appearing as two seperate nodes
    df[user_col] = df[user_col].astype(str).str.strip().str.lower()

    #Convert the time column to datetime format
    if time_col:
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    
    #One column for VADER sentiment, and another column for TF-IDF algorithm
    df['vader_text'] = df[text_col].apply(clean, heavy=False, platform=platform)
    df['cleaned_text'] = df[text_col].apply(clean, heavy=True, platform=platform)

    return df[df['cleaned_text'].str.strip() != '']

    
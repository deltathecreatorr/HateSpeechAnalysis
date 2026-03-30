from dataset import getRedditDataframe, getTwitterDataframe
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

def clean_text(text) -> str:
    """
    Cleans the input text by performing a series of preprocessing steps such as lowercasing, removing URLs, mentions, special characters, and stop words.
    Arguments:
        text: A string representing the text to be cleaned.
    Returns:
        A string representing the cleaned text.
    """
    text = text.lower()

    text = re.sub(r'http\S|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'@\w+', '', text)

    text = re.sub(r'[^a-z\s]', '', text)

    words = text.split()
    meaningful_words = [word for word in words if word not in stop_words]
    return ' '.join(meaningful_words)

def preprocessRedditDataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the Reddit dataframe by tokenisation.
    
    Arguments:
        df: A pandas Dataframe containing the Reddit dataset with the following columns: "author_name", "post_id", "comment_id", "self_text", "created_time", "user_total_karma", "user_account_created_time" the dataframe can be accessed by calling reddit_dataset['column_name'] where column_name is one of the columns mentioned above.
    Returns:
        A pandas Dataframe containing the preprocessed Reddit dataset with the following columns: "author_name", "post_id", "comment_id", "self_text", "created_time", "user_total_karma", "user_account_created_time" the dataframe can be accessed by calling reddit_dataset['column_name'] where column_name is one of the columns mentioned above.
    """
    bot_list = ['automoderator', 'suggestsizebot', 'helperbot_']
    df = df[~df['author_name'].str.lower().isin(bot_list)]
    df = df.dropna(subset=['self_text'])
    df = df[df['self_text'].str.strip() != '']

    df['self_text'] = df['self_text'].apply(clean_text)

    df['author_name'] = df['author_name'].astype(str).str.lower().str.strip()
    return df

def preprocessTwitterDataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the Twitter dataframe by tokenisation.
    
    Arguments:
        df: A pandas Dataframe containing the Twitter dataset with the following columns: 'tweet_id', 'user_id', 'username', 'text', 'created_at', 'retweets', 'likes', 'mentions', 'retweeted_user', 'quoted_user', 'followers', 'account_created', 'has_media', 'reply_to', 'hashtags' the dataframe can be accessed by calling twitter_dataset['column_name'] where column_name is one of the columns mentioned above.
    Returns:
        A pandas Dataframe containing the preprocessed Twitter dataset with the following columns: 'tweet_id', 'user_id', 'username', 'text', 'created_at', 'retweets', 'likes', 'mentions', 'retweeted_user', 'quoted_user', 'followers', 'account_created', 'has_media', 'reply_to', 'hashtags' the dataframe can be accessed by calling twitter_dataset['column_name'] where column_name is one of the columns mentioned above.
    """
    df = df.dropna(subset=['text'])
    df = df[df['text'].str.strip() != '']

    df['text'] = df['text'].apply(clean_text)
    df = df[df['text'].str.strip() != '']

    df['username'] = df['username'].astype(str).str.lower().str.strip()
    
    return df
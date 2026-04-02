
import pandas as pd
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
twikit_path = os.path.abspath(os.path.join(current_dir, '..', 'libs', 'twikit'))
sys.path.insert(0, twikit_path)
from dotenv import load_dotenv
from twikit import Client
import asyncio
import csv
import time

load_dotenv()

#all the events and their date ranges for before and after an event.
event_dates = {
    "October_7": {"since":"2023-10-06", "until":"2023-10-12"},
    "Al-Ahli_Hospital": {"since":"2023-10-16", "until":"2023-10-22"},
    "Iran_Strikes": {"since":"2024-09-30", "until":"2024-10-06"},
    "ICC_Arrest": {"since":"2024-11-20", "until":"2024-11-26"},
    "Doha_Attack": {"since":"2025-09-08", "until":"2025-09-14"}
}


def getRedditDataframe() -> None:
    """
    Returns the CSV Reddit file data as a Pandas Dataframe.

    Arguments:
        None
    Returns:
        None, but the function will create multiple CSV files in the dataset/ directory, each file will be named reddit_{event_name}_{data_type}.csv where event_name is one of the events from the
    """
    
    df = pd.read_csv('dataset/reddit_opinion_PSE_ISR.csv', usecols = ["author_name", "post_id", "comment_id", "self_text", "created_time", "user_total_karma", "user_account_created_time", "subreddit"])

    df['created_time'] = pd.to_datetime(df['created_time'], errors='coerce')
    df = df.dropna(subset=['created_time'])

    output_dir = 'dataset/'

    #Iterate through each event and create separate CSV files for baseline and event data based on the defined date ranges.
    for event, dates in event_dates.items():
        since_dt = pd.to_datetime(dates['since'])
        event_dt = since_dt + pd.Timedelta(days=1)
        until_dt = pd.to_datetime(dates['until'])

        baseline_mask = (df['created_time'] >= since_dt) & (df['created_time'] < event_dt)
        baseline_df = df[baseline_mask]
        base_filename = f"reddit_{event}_BASELINE.csv"
        baseline_df.to_csv(os.path.join(output_dir, base_filename), index=False)

        event_mask = (df['created_time'] >= event_dt) & (df['created_time'] <= until_dt)
        event_df = df[event_mask]
        event_filename = f"reddit_{event}_EVENT.csv"
        event_df.to_csv(os.path.join(output_dir, event_filename), index=False)

def getTwitterDataset(filepath: str, search_query: str, max_tweets: int = 5000) -> None:
    """
    Scrape Twitter Data using TwiKit library. 
    The tweets will be stored into a CSV to create my own dataset,
    but adding twitter credentials to a .env file at the
    root directory will also allow you to run this script.
    
    Arguments:
        filepath: A string representing the path where the twitter_dataset.csv will be stored.
    Returns:
        None, but the function will create a CSV file named twitter_dataset.csv in the filepath from the Arguments.
    """
    
    client = Client("en-US")
    file_exists = os.path.isfile(filepath)
    headers = ['tweet_id', 'user_id', 'username', 'text', 'created_at', 'retweets', 'likes', 'mentions', 'retweeted_user', 'quoted_user', 'followers', 'account_created', 'has_media', 'reply_to', 'hashtags']

    #Scrape Twitter data using TwiKit
    async def main():
        try:
            client.load_cookies("cookies.json")
        except FileNotFoundError or Exception as e:
            await client.login(
                auth_info_1 = os.getenv("USERNAME"),
                auth_info_2 = os.getenv("EMAIL"),
                password = os.getenv("PASSWORD"),
                cookies_file = "cookies.json"
            )
            client.save_cookies("cookies.json")
        
        encoded_query = search_query.replace(" ", "%20").replace(":", "%3A").replace("\"", "%22")
        debug_url = f"https://x.com/search?q={encoded_query}&f=live"
        print(f"DEBUG: Try opening this in your browser: {debug_url}")
        tweets = await client.search_tweet(search_query, "Latest", count=100)
        
        #Filter out any news accounts to reduce noise in the dataset.
        news_indicators = ['news', 'breaking', 'report', 'official', 'press', 'daily', 'wire', 'gazette', 'times', 'journal']

        with open(filepath, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()

            count = 0
            while tweets and count < max_tweets:
                for tweet in tweets:
                    
                    #Apply filters to exclude news accounts, or followers with a large following
                    username_lower = tweet.user.screen_name.lower()
                    if any(indicator in username_lower for indicator in news_indicators):
                        continue

                    if tweet.user.followers_count > 50000 and tweet.user.following_count < 500:
                        continue

                    if tweet.text.startswith("BREAKING:") or tweet.text.startswith("REPORT:") or tweet.text.startswith("LIVE"):
                        continue

                    if len(tweet.text.split()) < 4:
                        continue
                    
                    entities = getattr(tweet, 'entities', {})
                    mentions_list = entities.get('user_mentions', [])
                    
                    #Handle exceptions when accessing retweeted or quoted users, may be empty due to privacy settings
                    retweeted_username = ""
                    try:
                        if hasattr(tweet, 'retweeted_tweet') and tweet.retweeted_tweet:
                            retweeted_username = tweet.retweeted_tweet.user.screen_name
                    except (KeyError, AttributeError, Exception):
                        retweeted_username = "hidden_or_suspended"
                    
                    quoted_username = ""
                    try:
                        if hasattr(tweet, 'quote') and tweet.quote:
                            quoted_username = tweet.quote.user.screen_name
                    except (KeyError, AttributeError, Exception):
                        quoted_username = "hidden_or_suspended"

                    #handle cases where the tweet is a reply to another user
                    reply_to_user = getattr(tweet, 'in_reply_to_screen_name', "")
                    writer.writerow({
                        'tweet_id': tweet.id,
                        'user_id': tweet.user.id,
                        'username': tweet.user.screen_name,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'retweets': tweet.retweet_count,
                        'likes': tweet.favorite_count,
                        'mentions': [m['screen_name'] for m in mentions_list],
                        'retweeted_user': retweeted_username,
                        'quoted_user': quoted_username,
                        'followers': tweet.user.followers_count,
                        'account_created': tweet.user.created_at,
                        'has_media': 1 if getattr(tweet, 'media', None) else 0,
                        'reply_to': reply_to_user,
                        'hashtags': [h['text'] for h in entities.get('hashtags', [])]
                    })
                    count += 1

                if count >= max_tweets:
                    break
                
                #Wait to avoid rate limits, then get the next batch of tweets
                await asyncio.sleep(8)
                tweets = await tweets.next()
            
        print(f"Data has been successfully written to {filepath}")
    
    asyncio.run(main())

def createTwitterDataset() -> None:
    """
    Creates a Twitter csv file containing tweets related to the keywords from the Israel-Palestine conflict.
    Arguments:
        None
    Returns:    
        None, but the function will create multiple CSV files in the dataset/ directory, each file will be named twitter_{event_name}_{data_type}.csv where event_name is one of the events from the
    """
    #All keywords that are relevant for all the events in the timeline
    base_keywords = ["Israel", "Palestine", "Gaza", "Hamas"]
    base_query = " OR ".join(base_keywords)

    #Specific keywords for each event to capture more relevant tweets, added onto the base query
    specific_keywords_dict = {
        "October_7": ["October 7", "Oct 7", "Al-Aqsa Flood", "Hamas attack", "Nova Festival", "hostages", "kibbutz", "incursion", "Sderot", "Be'eri", "paragliders"],
        "Al-Ahli_Hospital": ["Al-Ahli", "Baptist Hospital", "hospital bombing", "hospital strike", "Gaza hospital", "PIJ", "misfire", "failed rocket", "war crime", "massacre"],
        "Iran_Strikes": ["Iran", "Tehran", "ballistic missiles", "missile strike", "Iron Dome", "Arrow 3", "Nevatim", "retaliation", "Islamic Republic", "supersonic"],
        "ICC_Arrest": ["ICC", "International Criminal Court", "arrest warrant", "war crimes", "The Hague", "Rome Statute", "crimes against humanity", "Yoav Gallant", "legal action"],
        "Doha_Attack": ["Doha", "Qatar", "targeted strike", "hotel attack", "assassination", "bombing", "Hamas leadership", "Doha explosion"]
    }

    #loop through each event and create seperate datasets for the baseline and event
    for event, keywords in specific_keywords_dict.items():
        before_date = event_dates[event]["since"]
        after_start = (pd.to_datetime(before_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        until_date = event_dates[event]["until"]

        baseline_query = f"({base_query}) since:{before_date} until:{after_start} lang:en"
        print(f"--- Scraping BASELINE for {event} ---")
        getTwitterDataset(f"dataset/twitter_{event}_BASELINE.csv", baseline_query, max_tweets=1000)

        event_query_str = " OR ".join([f'"{k}"' if " " in k else k for k in keywords])
        event_query = f"({base_query} OR {event_query_str}) since:{after_start} until:{until_date} lang:en"

        print(f"--- Scraping EVENT data for {event} ---")
        getTwitterDataset(f"dataset/twitter_{event}_EVENT.csv", event_query, max_tweets=2000)

        print(f"Finished collecting tweets for {event}")
        time.sleep(400)
 
if __name__ == "__main__":
    getRedditDataframe()




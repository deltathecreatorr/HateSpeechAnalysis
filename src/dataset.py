
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


def getRedditDataframe() -> pd.DataFrame:
    """
    Returns the CSV Reddit file data as a Pandas Dataframe.

    Arguments:
        None
    Returns:
        reddit_dataset: A pandas Dataframe containing the Reddit dataset with the following columns: "author_name", "post_id", "comment_id", "self_text", "created_time", "user_total_karma", "user_account_created_time" the dataframe can be accessed by calling reddit_dataset['column_name'] where column_name is one of the columns mentioned above.
    """
    df = pd.read_csv('dataset/reddit_opinion_PSE_ISR.csv', usecols = ["author_name", "post_id", "comment_id", "self_text", "created_time", "user_total_karma", "user_account_created_time"])
    return df

def getTwitterDataframe(filepath: str, search_query: str, max_tweets: int = 5000) -> None:
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
    headers = ['tweet_id', 'user_id', 'username', 'text', 'created_at', 'retweets', 'likes', 'mentions', 'retweeted_user', 'quoted_user', 'followers', 'account_created', 'has_media']

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

        tweets = await client.search_tweet(search_query, "Latest", count=50)

        news_indicators = ['news', 'breaking', 'report', 'official', 'press', 'daily', 'wire', 'gazette', 'times', 'journal']

        with open(filepath, mode="a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()

            count = 0
            while tweets and count < max_tweets:
                for tweet in tweets:
                    
                    username_lower = tweet.user.screen_name.lower()
                    if any(indicator in username_lower for indicator in news_indicators):
                        continue

                    if tweet.user.followers_count > 50000 and tweet.user.following_count < 500:
                        continue

                    if tweet.text.startswith("BREAKING:") or tweet.text.startswith("REPORT:") or tweet.text.startswith("LIVE"):
                        continue
                    
                    entities = getattr(tweet, 'entities', {})
                    mentions_list = entities.get('user_mentions', [])

                    retweeted_username = ""
                    if hasattr(tweet, 'retweeted_tweet') and tweet.retweeted_tweet:
                        retweeted_username = tweet.retweeted_tweet.user.screen_name
                    
                    quoted_username = ""
                    if hasattr(tweet, 'quote') and tweet.quote:
                        quoted_username = tweet.quote.user.screen_name

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
                        'has_media': 1 if getattr(tweet, 'media', None) else 0
                    })
                    count += 1

                if count >= max_tweets:
                    break

                await asyncio.sleep(5)
                tweets = await tweets.next()
            
        print(f"Data has been successfully written to {filepath}")
    
    asyncio.run(main())

def createTwitterDataset():
    """"""
    base_keywords = ["Israel", "Palestine", "Gaza", "Hamas", "IDF", "Zionism", "Netanyahu", "West Bank", "Anti-Semitism", "Anti-Semitic", "Genocide", "Occupation", "Terrorism", "Apartheid", "Resistance", "Settlers", "Retaliation", "War Crimes", "#FreePalestine", "#StandWithIsrael", "GazaUnderAttack", "IsraelUnderAttack"]
    base_query = " OR ".join(base_keywords)

    specific_keywords_dict = {
        "October_7": ["October 7", "Oct 7", "Al-Aqsa Flood", "Hamas attack", "Nova Festival", "hostages", "kibbutz", "incursion", "Sderot", "Be'eri", "paragliders"],
        "Al-Ahli_Hospital": ["Al-Ahli", "Baptist Hospital", "hospital bombing", "hospital strike", "Gaza hospital", "PIJ", "misfire", "failed rocket", "war crime", "massacre"],
        "Iran_Strikes": ["Iran", "Tehran", "ballistic missiles", "missile strike", "Iron Dome", "Arrow 3", "Nevatim", "retaliation", "Islamic Republic", "supersonic"],
        "ICC_Arrest": ["ICC", "International Criminal Court", "arrest warrant", "war crimes", "The Hague", "Rome Statute", "crimes against humanity", "Yoav Gallant", "legal action"],
        "Doha_Attack": ["Doha", "Qatar", "targeted strike", "hotel attack", "assassination", "bombing", "Hamas leadership", "Doha explosion"]
    }

    event_dates = {
        "October_7": {"since":"2023-10-06", "until":"2023-10-12"},
        "Al-Ahli_Hospital": {"since":"2023-10-16", "until":"2023-10-22"},
        "Iran_Strikes": {"since":"2024-09-30", "until":"2024-10-06"},
        "ICC_Arrest": {"since":"2024-11-20", "until":"2024-11-26"},
        "Doha_Attack": {"since":"2025-09-08", "until":"2025-09-14"}
    }

    tweet_count = 0
    for event, keywords in specific_keywords_dict.items():
        before_date = event_dates[event]["since"]
        after_start = (pd.to_datetime(before_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        until_date = event_dates[event]["until"]

        baseline_query = f"({base_query}) since:{before_date} until:{after_start} lang:en (filter:replies OR filter:quotes)"
        print(f"--- Scraping BASELINE for {event} ---")
        getTwitterDataframe(f"dataset/twitter_{event}_BASELINE.csv", baseline_query, max_tweets=1000)

        event_query_str = " OR ".join([f'"{k}"' if " " in k else k for k in keywords])
        event_query = f"({base_query} OR {event_query_str}) since:{after_start} until:{until_date} lang:en (filter:replies OR filter:quotes)"
    
        print(f"--- Scraping EVENT data for {event} ---")
        getTwitterDataframe(f"dataset/twitter_{event}_EVENT.csv", event_query, max_tweets=2000)

        print(f"Finished collecting tweets for {event}. Total tweets collected so far: {tweet_count}")
    

if __name__ == "__main__":
    createTwitterDataset()
    




import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
twikit_path = os.path.abspath(os.path.join(current_dir, '..', 'libs', 'twikit'))
sys.path.insert(0, twikit_path)

from twikit import Client
import asyncio


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

def getTwitterDataframe(filepath: str, search_query: str) -> None:
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

        tweets = await client.search_tweet(search_query, "Latest") 

        for tweet in tweets:
            print(tweet)
    
    asyncio.run(main())

if __name__ == "__main__":
    getRedditDataframe()
    getTwitterDataframe("dataset/twitter_dataset.csv", "hate speech")




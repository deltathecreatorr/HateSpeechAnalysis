import pandas as pd
import os
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from preprocessing import process_dataframe
from dataset import event_dates

nltk.download('vader_lexicon')

def vader_analysis() -> None:
    """
    Performs VADER sentiment analysis on uncleaned data from 
    
    Keyword arguments:
    argument -- description
    Return: return_description
    """
    for platform in ['reddit', 'twitter']:
        for keys, _ in event_dates.items():
            baseline_path = f"dataset/{platform}_{keys}_BASELINE.csv"
            event_path = f"dataset/{platform}_{keys}_EVENT.csv"

            if os.path.exists(baseline_path) and os.path.exists(event_path):
                
                baseline_df = pd.read_csv(baseline_path)
                event_df = pd.read_csv(event_path)

                #Preprocess the data for VADER sentiment analysis
                baseline_df = process_dataframe(baseline_df, platform)
                event_df = process_dataframe(event_df, platform)

                for df in [baseline_df, event_df]:
                    #Calculate the VADER sentiment scores for each text
                    df['sentiment_score'] = df['vader_text'].apply(
                        lambda x: SentimentIntensityAnalyzer().polarity_scores(x)['compound']
                    )
                
                #Save the VADER sentiment scores to new CSV files
                baseline_df.to_csv(f"dataset/{platform}_{keys}_BASELINE_VADER.csv", index=False)
                event_df.to_csv(f"dataset/{platform}_{keys}_EVENT_VADER.csv", index=False)

                #Calculate the mean sentiment scores before and after the event
                b_mean = baseline_df['sentiment_score'].mean()
                e_mean = event_df['sentiment_score'].mean()
                print(f"{platform.upper()} - {keys} - Baseline Mean Sentiment: {b_mean:.4f}, Event Mean Sentiment: {e_mean:.4f}")

if __name__ == "__main__":
    vader_analysis()


        
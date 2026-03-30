import pandas as pd
import os
from dataset import event_dates
from scipy import stats

def analyse_sentiment_changes() -> list[list]:
    """
    Analyses the sentiment changes from before the event and during/after it
    
    Arguments:
        None
    Returns: 
        A 2D list of the sentiment changes for each platform and event
    """
    sentiment_changes = []
    for platform in ['reddit', 'twitter']:
        for keys, _ in event_dates.items():
            baseline_path = f"dataset/{platform}_{keys}_BASELINE_VADER.csv"
            event_path = f"dataset/{platform}_{keys}_EVENT_VADER.csv"

            if os.path.exists(baseline_path) and os.path.exists(event_path):
                baseline_df = pd.read_csv(baseline_path)
                event_df = pd.read_csv(event_path)

                b_mean = baseline_df['sentiment_score'].mean()
                e_mean = event_df['sentiment_score'].mean()
                change = e_mean - b_mean

                t_stat, p_val = stats.ttest_ind(
                    baseline_df['sentiment_score'],
                    event_df['sentiment_score'],
                    equal_var=False
                )

                sentiment_changes.append([platform, keys, b_mean, e_mean, change, p_val])

                sig_label = "SIGNIFICANT" if p_val < 0.05 else "NOT SIGNIFICANT"
                print(f"{platform.upper()} - {keys}:")
                print(f"  Change: {change:.4f} | P-value: {p_val:.4e} ({sig_label})")
                print("-" * 30)
    
    return sentiment_changes

results = analyse_sentiment_changes()
df_results = pd.DataFrame(results, columns=['Platform', 'Event', 'Base_Mean', 'Event_Mean', 'Shift', 'P_Value'])
df_results.to_csv('dataset/FINAL_STATISTICAL_RESULTS.csv', index=False)

    

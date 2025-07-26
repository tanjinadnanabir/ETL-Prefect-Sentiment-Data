from prefect import flow, task
from datetime import timedelta
import datetime
import pandas as pd
import csv
import re
import time

# Task: Extract
@task
def extract_data():
    column = ['target','id','date','flag','user','text']
    df = pd.read_csv('dataset/tweets.csv', encoding='latin1', names=column)
    return df

def remove_emojis(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

# Task: Transform
@task
def transform_data(df):
    df['sentiment'] = df['target'].map({0:'negative', 2:'neutral', 4:'positive'})
    df['text'] = df['text'].str.lower()
    df['text'] = df['text'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))  # Remove URLs
    df['text'] = df['text'].apply(lambda x: re.sub(r'@\w+', '', x))  # Remove mentions
    df['text'] = df['text'].apply(lambda x: re.sub(r'#\w+', '', x))  # Remove hashtags
    df['text'] = df['text'].apply(lambda x: re.sub(r'\d+', '', x))  # Remove digits
    df['text'] = df['text'].apply(lambda x: re.sub(r'[^\w\s]', '', x))  # Remove punctuation
    df['text'] = df['text'].str.strip()  # Remove leading and trailing whitespace
    df['text'] = df['text'].apply(lambda x: re.sub(r'\s+', ' ', x))  # Replace multiple spaces with a single space
    df['text'] = df['text'].apply(remove_emojis)
    df = df[['target', 'text']]
    return df

# Task: Load
@task
def load_data(df):
    df.to_csv('dataset/tweets_prefect.csv', index=False, encoding='utf-8')

# Main flow
@flow(name="ETL")
def data_etl_flow():
    raw = extract_data()
    cleaned = transform_data(raw)
    load_data(cleaned)

# Optional: You can test run locally
if __name__ == "__main__":
    data_etl_flow()

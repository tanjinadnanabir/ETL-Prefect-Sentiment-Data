from prefect import flow, task
import pandas as pd
import re
import os

# --- Task: Extract ---
@task(name="Extract Data")
def extract_data() -> pd.DataFrame:
    """Read tweets.csv from dataset folder."""
    file_path = os.path.join("dataset", "tweets.csv")
    print("Current working directory:", os.getcwd())
    print("Looking for file at:", file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    column_names = ['target', 'id', 'date', 'flag', 'user', 'text']
    df = pd.read_csv(file_path, encoding='latin1', names=column_names)
    return df

# --- Helper: Remove Emojis ---
def remove_emojis(text: str) -> str:
    emoji_pattern = re.compile(
        "[" u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"      # symbols & pictographs
        u"\U0001F680-\U0001F6FF"      # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"      # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

# --- Task: Transform ---
@task(name="Transform Data")
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize tweet text."""
    df['sentiment'] = df['target'].map({0: 'negative', 2: 'neutral', 4: 'positive'})
    df['text'] = df['text'].astype(str).str.lower()
    df['text'] = df['text'].apply(lambda x: re.sub(r'https?://\S+|www\.\S+', '', x))  # Remove URLs
    df['text'] = df['text'].apply(lambda x: re.sub(r'@\w+', '', x))  # Remove mentions
    df['text'] = df['text'].apply(lambda x: re.sub(r'#\w+', '', x))  # Remove hashtags
    df['text'] = df['text'].apply(lambda x: re.sub(r'\d+', '', x))  # Remove digits
    df['text'] = df['text'].apply(lambda x: re.sub(r'[^\w\s]', '', x))  # Remove punctuation
    df['text'] = df['text'].str.strip()
    df['text'] = df['text'].apply(lambda x: re.sub(r'\s+', ' ', x))
    df['text'] = df['text'].apply(remove_emojis)
    
    return df[['target', 'text', 'sentiment']]

# --- Task: Load ---
@task(name="Load Data")
def load_data(df: pd.DataFrame, output_path: str = 'dataset/tweets_prefect.csv'):
    """Save transformed DataFrame to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"✅ Cleaned data saved to {output_path}")

# --- Flow: Main ---
@flow(name="Tweet ETL Pipeline")
def data_etl_flow():
    raw_df = extract_data()
    cleaned_df = transform_data(raw_df)
    load_data(cleaned_df)

# --- Run Flow Manually ---
if __name__ == "__main__":
    data_etl_flow()

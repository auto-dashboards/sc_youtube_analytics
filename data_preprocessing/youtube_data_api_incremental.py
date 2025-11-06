from googleapiclient.discovery import build 
from dotenv import load_dotenv
import os
from isodate import parse_duration
import pandas as pd
import psycopg2
import json
import io


# === Load environment variables ===
load_dotenv()
api_key = os.getenv('YI_API_KEY')
channel_id = os.getenv('CHANNEL_KEY')

# === Connect to YouTube API ===
youtube = build('youtube', 'v3', developerKey=api_key)

# === Connect to Postgres ===
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()

# === Load youtube load metadata table and get latest date ===
df_metadata = pd.read_sql("SELECT * FROM stage.youtube_load_metadata;", conn)

max_load_date = df_metadata['last_run_date'].max()
max_load_date_str = max_load_date.isoformat().replace('+00:00', 'Z')


# === Run youtube search API to retrieve video id's for videos published after max load date ===
search_response = youtube.search().list(
    part="id",
    channelId=channel_id,
    type="video",
    order="date",
    publishedAfter=max_load_date_str
).execute()


# === Extract video IDs ===
video_ids = [video['id']['videoId'] for video in search_response['items']]


# === Batch video requests (50 IDs per call) ===
def batch_video(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

records = []
for batch in batch_video(video_ids, 50):
    video_response = youtube.videos().list(
        part="snippet,statistics,contentDetails,topicDetails",
        id=",".join(batch), 
    ).execute()

    for item in video_response.get('items', []):
        json_blob = json.dumps(item)
        records.append(json_blob)


# === Convert list of dicts to DataFrame ===
df_data_inc = pd.DataFrame(records, columns=['json_rows'])


# === Append to stage table ===
buffer = io.StringIO()
df_data_inc.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cur.copy_expert(
    "COPY stage.sc_yt_video_data (json_rows) FROM STDIN WITH CSV HEADER", ## specify json_rows so it knows which column to fill and rest will take default value
    buffer
) 

conn.commit()
cur.close()
conn.close()









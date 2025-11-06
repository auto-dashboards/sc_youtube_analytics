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


# === Get Uploads Playlist ID ===
channel_response = youtube.channels().list(
    part="contentDetails",
    id=channel_id
).execute()

playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 


# === Retrieve all video IDs from playlist ===
all_video_info = []
next_page_token = None

while True: 
    playlist_response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50, 
            pageToken=next_page_token
    ).execute()

    all_video_info.append(playlist_response['items'])
    next_page_token = playlist_response.get('nextPageToken')

    if not next_page_token:
        break


# === Extract video IDs ===
video_ids = [video['contentDetails']['videoId'] for batch in all_video_info for video in batch]


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
df_data = pd.DataFrame(records, columns=['json_rows'])


# === Retrieve max published date of videos ===
published_dates = []
for rec in records:
    record_dict = json.loads(rec)
    published_dt = record_dict['snippet'].get('publishedAt')
    published_dates.append(published_dt)

max_publish_dt = max(published_dates)


# === Convert dates in to DataFrame ===
df_dates = pd.DataFrame({'source_system': ['YOUTUBE_DATA_API'], 
                         'last_run_date': [max_publish_dt], 
                         'record_count': [len(published_dates)], 
            })


# === Connect to Postgres ===
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()


# === Copy video data into memory buffer ===
cur.execute("TRUNCATE TABLE stage.sc_yt_video_data;") ## since full refresh, truncate table first

buffer = io.StringIO()
df_data.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cur.copy_expert(
    "COPY stage.sc_yt_video_data (json_rows) FROM STDIN WITH CSV HEADER", ## specify json_rows so it knows which column to fill and rest will take default value
    buffer
) 

# === Copy load metadata into memory buffer ===
cur.execute("TRUNCATE TABLE stage.youtube_load_metadata;") ## since full refresh, truncate table first

buffer = io.StringIO()
df_dates.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cur.copy_expert(
    "COPY stage.youtube_load_metadata (source_system, last_run_date, record_count) FROM STDIN WITH CSV HEADER", ## specify json_rows so it knows which column to fill and rest will take default value
    buffer
) 

conn.commit()
cur.close()
conn.close()









# video_id = []
# video_dt = []
# video_duration = []
# video_title = [] 
# video_desc = []
# video_views = []
# video_likes = []
# video_comments = []
# video_category = []
# video_topics = []

# for batch in all_video_info:
#     video_id_list = [video['contentDetails']['videoId'] for video in batch]

#     video_response = youtube.videos().list(
#         part="snippet,statistics,contentDetails,topicDetails",
#         id=",".join(video_id_list)
#     ).execute()

#     for video in video_response['items']:

#         duration_sec = int(parse_duration(video['contentDetails'].get('duration')).total_seconds())
#         video_duration.append(duration_sec)

#         video_views.append(video['statistics'].get('viewCount'))
#         video_likes.append(video['statistics'].get('likeCount'))
#         video_comments.append(video['statistics'].get('commentCount'))
#         video_category.append(video['snippet'].get('categoryId'))
#         video_topics.append(video['topicDetails'].get('topicCategories'))
#         video_id.append(video.get('id'))
#         video_dt.append(video['snippet'].get('publishedAt'))
#         video_title.append(video['snippet'].get('title'))
#         video_desc.append(video['snippet'].get('description'))


# df_videos = pd.DataFrame({
#     'video_id': video_id, 
#     'video_title': video_title, 
#     'video_published_at': video_dt, 
#     'video_duration_sec': video_duration,
#     'video_description': video_desc, 
#     'video_category': video_category,
#     'video_topic': video_topics, 
#     'video_view_count': video_views,
#     'video_like_count': video_likes, 
#     'video_comment_count': video_comments, 
# })


# ## Load data into SQL 

# from psycopg2 import sql
# import json
# import io

# def df_to_stage(df, api_name):
#     json_rows = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
#     load_ts = datetime.now().isoformat()

#     df_stage = pd.DataFrame({
#         'json_rows': json_rows, 
#         'api_name': api_name, 
#         'load_ts': load_ts
#     })

#     return df_stage


# def get_connection():
#     return psycopg2.connect(
#         host=os.getenv("DB_HOST"),
#         port=os.getenv("DB_PORT"),
#         database=os.getenv("DB_NAME"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD")
#     )


# def run_sql_query(query):
#     conn = get_connection()
#     cur = conn.cursor()
#     # query = sql.SQL(query).format(table_name)
#     cur.execute(query)
#     conn.commit()
#     cur.close()
#     conn.close()


# def create_stage_table(table_name):

#     # table_name = sql.Identifier('stage', f'{table_name}')

#     query = sql.SQL("""
#         DROP TABLE IF EXISTS stage.{tbl} CASCADE;
#         CREATE TABLE stage.{tbl} (
#             json_rows   JSONB,
#             api_name    TEXT,
#             load_ts     TIMESTAMPTZ DEFAULT NOW()
#         );      
#     """).format(tbl=sql.Identifier(table_name))

#     run_sql_query(query)
#     print("Stage table created successfully")


# def load_to_stage(df, table_name):
#     conn = get_connection() # Establish connection to PostgreSQL database
#     cur = conn.cursor() # Create a cursor object to execute SQL commands

#     cur.execute("SET search_path TO stage;") # Set the path to stage. PostgreSQL will look for tables inside stage
#     conn.commit() # Saves this setting for the current session. When you run SQL commands, those changes are made in temp state called a transaction. Without commiting, all changes will be lost if the connection closes or if there is an error

#     buffer = io.StringIO() # Creates a 'fake file' in your computer's memory, all stored in RAM, not on hard drive 
#     df.to_csv(buffer, index=False, header=True) # This writes the df into that 'fake file' in CSV format 
#     buffer.seek(0) # When you write to a 'fake file', the 'cursor' moves to the end. This command moves the cursor back to the start, so when you read from this buffer, it reads from the beginning

#     cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", buffer) # This copy_expert command requires a file-like object to read CSV. This stops you saving directly on hard drive, faster and cleaner processing

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Table populated successfully")


# df_videos_json = df_to_stage(df_videos, 'data_api')
# create_stage_table('sc_yt_video_data')
# load_to_stage(df_videos_json, 'sc_yt_video_data')



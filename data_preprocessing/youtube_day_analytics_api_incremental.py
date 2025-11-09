from googleapiclient.discovery import build 
from pprint import pprint
from dotenv import load_dotenv
import os
from datetime import datetime, date
from isodate import parse_duration
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import os
import json
import io
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# === Load environment variables ===
load_dotenv()
api_key = os.getenv('YI_API_KEY')
channel_id = os.getenv('CHANNEL_KEY')
client_id = os.environ["YOUTUBE_CLIENT_ID"]
client_secret = os.environ["YOUTUBE_CLIENT_SECRET"]
refresh_token = os.environ["YOUTUBE_REFRESH_TOKEN"]

# === Connect to Neon DB on Postgres ===
conn = psycopg2.connect(os.environ['DBL_URL'])
cur = conn.cursor()

# === Load youtube load metadata table and get latest date ===
df_metadata = pd.read_sql("SELECT * FROM stage.youtube_load_metadata where stage_table = 'sc_yt_day_data';", conn)

max_load_date = df_metadata['last_run_date'].max()
max_load_date_str = max_load_date.strftime('%Y-%m-%d')

# === Connect to YouTube Analytics API ===
credentials = Credentials(
    None,
    refresh_token=refresh_token,
    token_uri="https://oauth2.googleapis.com/token",
    client_id=client_id,
    client_secret=client_secret
)

# === Refresh access token automatically ===
credentials.refresh(Request())

# === Build Youtube Analytics client ===
analytics = build('youtubeAnalytics', 'v2', credentials=credentials)


# === Retrieve day level metrics, convert into dicts and then store in a DF ===
start_date = max_load_date_str  # YouTube's earliest possible date
end_date = date.today().isoformat()

metrics = (
    "views,likes,dislikes,comments,shares,"
    "estimatedMinutesWatched,averageViewDuration,"
    "averageViewPercentage,subscribersGained,subscribersLost,"
    "annotationClickThroughRate,annotationCloseRate,"
    "cardClicks,cardTeaserClicks,cardImpressions,cardTeaserImpressions,"
)

metric_response = analytics.reports().query(
    ids='channel==MINE',
    startDate=start_date,
    endDate=end_date,
    metrics=metrics,
    dimensions="day",
).execute()

col_headers = [col.get('name', []) for col in metric_response['columnHeaders']]

records = []
for row in metric_response.get('rows', []):
    record = dict(zip(col_headers, row))
    records.append(json.dumps(record))

df_day_metrics = pd.DataFrame(records, columns=['json_rows'])


# === Append to stage table ===
buffer = io.StringIO()
df_day_metrics.to_csv(buffer, index=False, header=True)
buffer.seek(0)

cur.copy_expert(
    "COPY stage.sc_yt_day_data (json_rows) FROM STDIN WITH CSV HEADER", ## specify json_rows so it knows which column to fill and rest will take default value
    buffer
) 

conn.commit()
cur.close()
conn.close()

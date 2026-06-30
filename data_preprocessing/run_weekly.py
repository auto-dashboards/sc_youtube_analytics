import os 
import argparse
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import helper_functions
import fetch_video_data
import fetch_day_data
import pandas as pd
import json
import video_timestamps

def main(mode):

    # === Load environment variables from .env file ===
    load_dotenv()
    api_key = os.getenv('YI_API_KEY')
    channel_id = os.getenv('CHANNEL_KEY')
    dbl_url = os.getenv('DBL_URL')
    print('Loaded environment variables')

    # === Fetch youtube video IDs ===
    video_ids = helper_functions.get_channel_videos_ids(api_key, channel_id)
    print('Fetched video IDs')

    # === Connect to the youtube data and youtube analytics API's ===
    youtube_api = helper_functions.connect_yt_data_api(api_key)
    print('Connected to Youtube Data API')

    df_transcript_all = []
    video_metrics = fetch_video_data.fetch_recent_videos(video_ids, youtube_api, lookback_days=7)
    model = video_timestamps.load_whisper_model()

    for id in video_metrics['video_id'].unique():
        url = f'https://www.youtube.com/watch?v={id}'
        video_timestamps.download_video_audio(url)
        video_timestamps.download_video_transcript(id, model)
        df_transcript = video_timestamps.video_transcript_clean(id)
        df_transcript_all.append(df_transcript)

    df_transcript_all = pd.concat(df_transcript_all, ignore_index=True)

    # === Insert transcript data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_video_transcript', df_transcript_all, mode)
    print('Inserted transcript data records into Postgres')


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--mode',
        choices=['append', 'truncate'],
        required=True
    )

    args = parser.parse_args()

    main(args.mode)

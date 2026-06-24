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
    client_id = os.getenv('YOUTUBE_CLIENT_ID')
    client_secret = os.getenv('YOUTUBE_CLIENT_SECRET')
    refresh_token = os.getenv('YOUTUBE_REFRESH_TOKEN')
    dbl_url = os.getenv('DBL_URL')
    print('Loaded environment variables')

    # === Fetch youtube video IDs ===
    video_ids = helper_functions.get_channel_videos_ids(api_key, channel_id)
    print('Fetched video IDs')

    # === Connect to the youtube data and youtube analytics API's ===
    youtube_api = helper_functions.connect_yt_data_api(api_key)
    analytics_api = helper_functions.connect_yt_analytics_api(refresh_token, client_id, client_secret)
    print('Connected to Youtube Data & Analytics APIs')

    # === Fetch youtube video metrics ===
    video_data = fetch_video_data.fetch_video_full_data(video_ids, youtube_api)
    video_min_data = fetch_video_data.fetch_video_min_data(video_ids, analytics_api)
    video_est_watched = fetch_video_data.fetch_video_est_watched(video_ids, analytics_api)
    print('Fetched all Youtube video data')

    # === Join video data into a single table ===
    video_metrics_comb = video_data.merge(video_min_data, left_on='video_id', right_on='video_id', how='left').merge(video_est_watched, left_on='video_id', right_on='video_id', how='left')
    video_metrics_comb = video_metrics_comb[['video_id', 'video_data', 'minute_metrics', 'estimatedMinutesWatched']]
    print('Combined Youtube video data')

    # === Fetch youtube day metrics ===
    day_metrics = fetch_day_data.fetch_day_full_data(analytics_api, mode)
    print('Fetched day metrics')

    # === Fetch video transcript - Step 1 - Fetch latest video id ===
    df_video_date = video_metrics_comb.copy()
    df_video_date['video_publish_dt'] = [json.loads(video_metrics_comb['video_data'][i])['snippet']['publishedAt'] for i in range(len(video_metrics_comb))]
    df_video_date = df_video_date[['video_id', 'video_publish_dt']].drop_duplicates()
    df_video_date['video_publish_dt'] = pd.to_datetime(df_video_date['video_publish_dt']).dt.date
    # Get the video ids published between prev Wednesday and the Thursday before
    days_since_weds = (date.today().weekday() - 2)
    most_recent_weds = (date.today()) - timedelta(days=days_since_weds)
    prev_thurs = most_recent_weds - timedelta(days=6)
    df_video_date = df_video_date[df_video_date['video_publish_dt'].between(prev_thurs, most_recent_weds)]

    # === Fetch video transcript - Step 2 - Fetch transcript of videos ===
    df_transcript_all = []
    video_ids_transcript = list(df_video_date['video_id'].unique())

    model = video_timestamps.load_whisper_model()

    for id in video_ids_transcript:
        url = f'https://www.youtube.com/watch?v={id}'
        video_timestamps.download_video_audio(url)
        video_timestamps.download_video_transcript(id, model)
        df_transcript = video_timestamps.video_transcript_clean(id)
        df_transcript_all.append(df_transcript)

    df_transcript_all = pd.concat(df_transcript_all, ignore_index=True)

    # === Insert video data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_video_data', video_metrics_comb, mode)
    print('Inserted video data records into Postgres')

    # === Insert day data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_day_data', day_metrics, mode)
    print('Inserted day data records into Postgres')

    # === Insert transcript data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_video_transcript', df_transcript_all, 'append')
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

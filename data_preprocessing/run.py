import os 
import argparse
from dotenv import load_dotenv
import helper_functions
import fetch_video_data
import fetch_day_data

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
    video_metrics_comb = video_data.merge(video_min_data, left_on='video_id', right_on='video_id', how='left').merge(video_est_watched, left_on='video_id', right_on='video', how='left')
    video_metrics_comb = video_metrics_comb[['video_id', 'video_data', 'minute_metrics', 'estimatedMinutesWatched']]
    print('Combined Youtube video data')

    # === Fetch youtube day metrics ===
    day_metrics = fetch_day_data.fetch_day_full_data(analytics_api, mode)
    print('Fetched day metrics')

    # === Insert video data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_video_data', video_metrics_comb, mode)
    print('Inserted video data records into Postgres')

    # === Insert day data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_day_data', day_metrics, mode)
    print('Inserted day data records into Postgres')


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--mode',
        choices=['append', 'truncate'],
        required=True
    )

    args = parser.parse_args()

    main(args.mode)

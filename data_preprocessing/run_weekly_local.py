import os 
import argparse
from dotenv import load_dotenv
from pathlib import Path
import boto3
import helper_functions
import fetch_video_data
import video_timestamps

def main(lookback_days):

    # === Load environment variables from .env file ===
    load_dotenv()
    api_key = os.getenv('YI_API_KEY')
    channel_id = os.getenv('CHANNEL_KEY')
    print('Loaded youtube environment variables')

    r2_account_id = os.getenv('R2_ACCOUNT_ID')
    r2_access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    r2_secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    r2_bucket_name = os.getenv('R2_BUCKET_NAME')
    print('Loaded Cloudflare R2 variables')

    s3 = boto3.client(
        's3', 
        endpoint_url=f'https://{r2_account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=r2_access_key_id,
        aws_secret_access_key=r2_secret_access_key
    )

    # === Fetch youtube video IDs ===
    video_ids = helper_functions.get_channel_videos_ids(api_key, channel_id)
    print('Fetched video IDs')

    # === Connect to the youtube data and youtube analytics API's ===
    youtube_api = helper_functions.connect_yt_data_api(api_key)
    print('Connected to Youtube Data API')

    video_metrics = fetch_video_data.fetch_recent_videos(video_ids, youtube_api, lookback_days)

    for id in video_metrics['video_id'].unique():
        url = f'https://www.youtube.com/watch?v={id}'
        video_timestamps.download_video_audio(url)

        video_path = Path('data_preprocessing') / 'video_audio' / f'{id}.mp4'

        BASE_DIR = Path(__file__).resolve().parent.parent
        VIDEO_AUDIO_DIR = BASE_DIR / "data_preprocessing" / "video_audio" / f'{id}.mp4'

        s3.upload_file(
            str(VIDEO_AUDIO_DIR),
            r2_bucket_name,
            f'incoming/{VIDEO_AUDIO_DIR.name}'
        )

        print(f'Uploaded {video_path.name} successfully!')


if __name__ == "__main__":
    
    # Create an object that can read inputs from the terminal
    # Example: run_weekly_local.py --lookback_days 10 
    parser = argparse.ArgumentParser(
        description='Download recent or all YouTube videos'
    )

    # Add an optional argument called lookback_days to define how many days to look back 
    parser.add_argument(

        # What the user types in the terminal
        '--lookback_days',

        # Convert the input into an integer
        type=int,

        # If the user doesn't specify anything, use 7
        default=7, 

        # Text shown when someone runs: python run_weekly_local.py --help
        help='Number of days to look back for downloading videos. 0 would download all'
    )

    # Read the terminal arguments
    args = parser.parse_args()

    # Run the pipeline 
    main(args.lookback_days)

from googleapiclient.discovery import build 
from dotenv import load_dotenv
import os
from isodate import parse_duration
import pandas as pd
from datetime import datetime, date
import psycopg2
import json
import io
from utils.helper_functions import get_channel_videos_ids, connect_yt_data_api, connect_yt_analytics_api


def fetch_video_ids():
        
    video_ids = get_channel_videos_ids(api_key, channel_id)

    return video_ids


def fetch_video_data(video_ids, youtube_api):

    # Batch video requests (50 IDs per call) and call fields
    def batch_video(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    records = []
    for batch in batch_video(video_ids, 50):
        video_response = youtube_api.videos().list(
            part="snippet,statistics,contentDetails,topicDetails,status,player",
            id=",".join(batch), 
        ).execute()

        for item in video_response.get('items', []):
            records.append(item)

    # Convert list of dicts to DataFrame
    video_metrics = pd.DataFrame(records)

    return video_metrics


def fetch_video_min_data(video_ids, analytics_api):

    start_date = '2022-09-16'  # YouTube's earliest possible date
    end_date = date.today().isoformat()

    all_videos_min_list = []
    for video_id in video_ids:
        response = analytics_api.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='averageConcurrentViewers,peakConcurrentViewers',
            dimensions='livestreamPosition',
            filters=f"video=={video_id}"
        ).execute()

        rows = response.get('rows', [])
        col_headers = [col.get('name', []) for col in response['columnHeaders']]

        if not rows:
            continue
        
        records = []
        for row in rows:
            record = dict(zip(col_headers, row))
            records.append(record)

        data = {
            'video_id': video_id, 
            'minute_metrics': [records],
        }

        videos_min = pd.DataFrame(data)
        all_videos_min_list.append(videos_min)

    videos_minute_metrics = pd.concat(all_videos_min_list, ignore_index=True)

    return videos_minute_metrics


def fetch_video_est_watched(video_ids, analytics_api):

    start_date = '2022-09-16'  # YouTube's earliest possible date
    end_date = date.today().isoformat()

    metrics = (
        "estimatedMinutesWatched"
    )

    response = analytics_api.reports().query(
        ids='channel==MINE',
        startDate=start_date,
        endDate=end_date,
        metrics=metrics,
        dimensions="video",
        filters="video==" + ",".join(video_ids)
    ).execute()

    rows = response.get('rows', [])
    col_headers = [col.get('name', []) for col in response['columnHeaders']]
    
    records = []
    for row in rows:
        record = dict(zip(col_headers, row))
        records.append(record)

    videos_est_watched = pd.DataFrame(records)

    return videos_est_watched


if __name__ == "__main__":

    # Load from .env file
    load_dotenv()
    api_key = os.getenv('YI_API_KEY')
    channel_id = os.getenv('CHANNEL_KEY')
    client_id = os.environ["YOUTUBE_CLIENT_ID"]
    client_secret = os.environ["YOUTUBE_CLIENT_SECRET"]
    refresh_token = os.environ["YOUTUBE_REFRESH_TOKEN"]

    video_ids = fetch_video_ids()
    youtube_api = connect_yt_data_api(api_key)
    analytics_api = connect_yt_analytics_api(api_key, refresh_token, client_id, client_secret)

    video_data = fetch_video_data(video_ids, youtube_api)
    video_min_data = fetch_video_min_data(video_ids, analytics_api)
    video_est_watched = fetch_video_est_watched(video_ids, analytics_api)

    video_metrics_comb = video_data.merge(video_min_data, left_on='id', right_on='video_id', how='left').merge(video_est_watched, left_on='id', right_on='video', how='left')




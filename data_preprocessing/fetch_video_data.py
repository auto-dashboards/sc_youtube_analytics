from googleapiclient.discovery import build 
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
import json
from helper_functions import get_channel_videos_ids, connect_yt_data_api, connect_yt_analytics_api, insert_records_to_postgres


def fetch_video_full_data(video_ids, youtube_api):

    '''
    Fetches video data for a list of Youtube Videos and returns as a Pandas Dataframe. I.e. Video metadata, video statistics etc 

    This function: 
    1. Splits the video list into batches of 50 (maximum allowed as input for the Youtube API)
    2. Fetches metadata for each batch using the Youtube Data API
    3. Stores each videos data as a JSON string in a Dataframe 
    4. Extracts the video ID into a separate column for each row 

    Args: 
        video_ids (list): List of Youtube video IDs 
        youtube_api: Authenticated Youtube API client for connection 

    Returns: 
        Dataframe with columns:
            video_data: JSON string containing full video metadata
            video_id: Youtube video ID

    '''

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
            records.append(json.dumps(item))

    # Convert list of json records to DataFrame
    video_metrics = pd.DataFrame(records, columns=['video_data'])
    video_metrics['video_id'] = [json.loads(record)['id'] for record in records]

    return video_metrics


def fetch_video_min_data(video_ids, analytics_api):

    '''
    Fetches video minute data for a list of Youtube videos and returns as a Pandas Dataframe. 
    I.e. for every 60 seconds, what are the average viewers and peak viewers

    This is separated from fetch_video_data() as you need to retrieve this data from the analytics api as opposed to data api
      
    This function: 
    1. For every video id, retrieve the average viewers and peak viewers per 60 seconds
    2. Stores each videos data as a JSON string in a Dataframe 
    3. Extracts the video ID into a separate column for each row 
    4. Concat them into a single dataframe

    Args: 
        video_ids (list): List of Youtube video IDs 
        analytics_api: Authenticated Youtube API client for connection 

    Returns: 
        Dataframe with columns:
            video_id: Youtube video ID
            minute_metrics: JSON string containing full video metadata
    '''

    start_date = '2022-09-16'
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
        
        records = {}
        for row in rows:
            record = dict(zip(col_headers, row))
            key = record['livestreamPosition']
            records[key] = record

        data = {
            'video_id': video_id, 
            'minute_metrics': json.dumps(records),
        }

        videos_min = pd.DataFrame([data])
        all_videos_min_list.append(videos_min)

    videos_minute_metrics = pd.concat(all_videos_min_list, ignore_index=True)

    return videos_minute_metrics


def fetch_video_est_watched(video_ids, analytics_api):

    '''
    Fetches video estimated watch time for a list of Youtube videos and returns as a Pandas Dataframe. 

    This is separated from fetch_video_data() as you need to retrieve this data from the analytics api as opposed to data api
    This is separated from fetch_video_min_data() as the dimensions field needs to be different and can't be joined together
      
    This function: 
    1. For every video id, retrieve the estimated watch time (mins)

    Args: 
        video_ids (list): List of Youtube video IDs 
        analytics_api: Authenticated Youtube API client for connection 

    Returns: 
        Dataframe with columns:
            video_id: Youtube video ID
            estimatedMinutesWatched: Estimated total watch time (mins)
    '''

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

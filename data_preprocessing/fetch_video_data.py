from googleapiclient.discovery import build 
from googleapiclient.errors import HttpError 
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
import json
import time
from helper_functions import safe_execute, get_channel_videos_ids, connect_yt_data_api, connect_yt_analytics_api, insert_records_to_postgres


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

    # Add try and except logic. If batch fails then add to a list. 3 retry attempts
    records = []
    failed_batches = []
    for batch in batch_video(video_ids, 50):
        try:
            video_response = safe_execute(
                youtube_api.videos().list(
                    part="snippet,statistics,contentDetails,topicDetails,status,player",
                    id=",".join(batch), 
                )
            )

            for item in video_response.get('items', []):
                records.append(json.dumps(item))

        except Exception as e:
            print(f'Batch failed: {batch} | Error: {e}')
            failed_batches.append(batch)

    # Convert list of json records to DataFrame. Single column of data. If it doesn't exist then return empty DF. Otherwise carry on with rest of code. 
    if not records:
        print('No video data to insert - skipping')
        return pd.DataFrame(columns=['video_id', 'video_data'])
    
    video_metrics = pd.DataFrame(records, columns=['video_data'])

    # Create a video id column for each video data row. Pull from video data column
    video_metrics['video_id'] = [json.loads(record).get('id') for record in records]

    print(f'Success: {len(records)} videos fetched')

    # If there are any failed batches, output the number of this
    if failed_batches:
        print(f'Failed batches: {len(failed_batches)}')

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
    failed_videos = []
    for video_id in video_ids:
        try: 
            response = safe_execute(
                analytics_api.reports().query(
                    ids='channel==MINE',
                    startDate=start_date,
                    endDate=end_date,
                    metrics='averageConcurrentViewers,peakConcurrentViewers',
                    dimensions='livestreamPosition',
                    filters=f"video=={video_id}"
                )
            )

            rows = response.get('rows', [])
            if not rows: # if data is empty, skip this video id and go to the next one in the try. Else carry on. Else is not required here when using continue
                continue

            col_headers = [col.get('name', []) for col in response['columnHeaders']]

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
    
        except Exception as e: 
            print(f'Video failed: {video_id} | Error: {e}')
            failed_videos.append(video_id)

    if not all_videos_min_list: # If no minute data, return an empty DF. Otherwise carry on
        print('No minute data to insert - skipping')
        return pd.DataFrame(columns=['video_id', 'minute_metrics'])
    
    videos_minute_metrics = pd.concat(all_videos_min_list, ignore_index=True)

    print(f"Success: {videos_minute_metrics['video_id'].nunique()} videos fetched")

    if failed_videos:
        print(f'Failed videos: {len(failed_videos)}')

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

    video_est_watched_list = []
    failed_videos = []
    for video_id in video_ids:
        try: 
            response = safe_execute(
                analytics_api.reports().query(
                    ids='channel==MINE',
                    startDate=start_date,
                    endDate=end_date,
                    metrics=metrics,
                    dimensions="video",
                    filters=f"video=={video_id}"
                )
            )

            rows = response.get('rows', [])
            if not rows: 
                continue

            data = {
                'video_id': rows[0][0], 
                'estimatedMinutesWatched': rows[0][1],
            }

            videos_est_watched = pd.DataFrame([data])
            video_est_watched_list.append(videos_est_watched)

        except Exception as e: 
            print(f'Video failed: {video_id} | Error: {e}')
            failed_videos.append(video_id)

    
    if not video_est_watched_list: # If no data, return an empty DF. Otherwise carry on
        print('No estimated min watched data to insert - skipping')
        return pd.DataFrame(columns=['video_id', 'estimatedMinutesWatched'])

    video_est_watched_all = pd.concat(video_est_watched_list, ignore_index=True)

    print(f"Success: {video_est_watched_all['video_id'].nunique()} videos fetched")

    if failed_videos:
        print(f'Failed videos: {len(failed_videos)}')

    return video_est_watched_all

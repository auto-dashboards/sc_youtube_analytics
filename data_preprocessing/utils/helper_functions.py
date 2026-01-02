from googleapiclient.discovery import build 
from dotenv import load_dotenv
import os
from isodate import parse_duration
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import psycopg2
import json
import io

def connect_yt_data_api(api_key):
    '''
    Returns a connection to the youtube data api
    '''
    youtube = build('youtube', 'v3', developerKey=api_key)

    return youtube


def connect_yt_analytics_api(api_key, refresh_token, client_id, client_secret):
    '''
    Returns a connection to the youtube analytics api
    '''

    credentials = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret
    )

    credentials.refresh(Request()) # Refresh access token automatically
    analytics = build('youtubeAnalytics', 'v2', credentials=credentials)

    return analytics


def get_channel_videos_ids(api_key, channel_id):

    '''
    Returns a list of all video IDs for a given youtube channel
    '''

    youtube = connect_yt_data_api(api_key)

    # Get uploads playlist ID ===
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] 

    # Retrieve all video IDs from playlist
    all_video_info = []
    next_page_token = None

    while True: 
        playlist_response = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50, 
                pageToken=next_page_token
        ).execute()

        all_video_info.append(playlist_response['items'])
        next_page_token = playlist_response.get('nextPageToken')

        if not next_page_token:
            break

    # Extract video IDs
    video_ids = [video['contentDetails']['videoId'] for batch in all_video_info for video in batch]

    return video_ids


    



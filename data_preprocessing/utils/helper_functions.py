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
    Creates a connection to the Youtube Data API
    
    Args: 
        api_key (str): Your Youtube Data API key
    
    Returns: 
        A youtube API client object

    '''
    youtube = build('youtube', 'v3', developerKey=api_key)

    return youtube


def connect_yt_analytics_api(refresh_token, client_id, client_secret):
    '''
    Creates a connection to the Youtube Analytics API

    Since the Youtube Analytics API requires OAuth2 authentication (i.e. asking the user to login and authorise access), 
    to stop manual requests, a refresh token alongisde client info is required to request an access token each time we run this API.

    None -> is a placeholder for the access token, but we will recieve this once we refresh

    Args:
        refresh_token (str): OAuth2 refresh token for the account
        client_id (str): OAuth2 client ID
        client_secret (str): OAuth2 client secret

    Returns: 
        A Youtube Analytics API client object   


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

    The API returns at most 50 videos per request (a rule by Youtube). 
    It starts with nextPageToken=None, which is referring to the first page, 
    and after it extracts the first 50 videos, it takes the next page token for the second page and so on. 
    Once it has extracted it all, next page token will be None to which the loop ends

    Args: 
        api_key (str): Your Youtube Data API key
        channel_id (str): Your Youtube channel ID

    
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


def insert_records_to_postgres(dbl_url, pg_table_name, df):

    '''
    Inserts records from a Pandas Dataframe into a PostgreSQL staging table. 

    This function performs a full refresh of the specified table: 
    1. Connects to the database using the provided URL
    2. Truncates the staging table to remove existing data
    3. Copies the dataframe into the table using an in memory CSV buffer. More efficient as it saves on your RAM instead of disk. 

    Args: 
        dbl_url (str): PostgreSQL connection URL
        pg_table_name (str): Name of the staging table to refresh 
        df: Pandas dataframe containing data to insert into the staging table 
    
    
    '''

    # === Connect to Neon DB on Postgres ===
    dbl_url = os.environ['DBL_URL']

    conn = psycopg2.connect(dbl_url)
    cur = conn.cursor()

    # === Copy video data into memory buffer ===
    truncate_message = f"TRUNCATE TABLE stage.{pg_table_name};"
    cur.execute(truncate_message) ## since full refresh, truncate table first

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    column_headers = df.columns
    column_headers = ", ".join(c for c in column_headers)

    copy_message = f"COPY stage.{pg_table_name} ({column_headers}) FROM STDIN WITH CSV HEADER"

    cur.copy_expert(
        copy_message, ## specify json_rows so it knows which column to fill and rest will take default value
        buffer
    ) 

    conn.commit()
    cur.close()
    conn.close()



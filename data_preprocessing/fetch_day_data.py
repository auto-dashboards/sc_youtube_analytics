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


def fetch_day_data(analytics_api):

    start_date = '2022-09-16'  # YouTube's earliest possible date
    end_date = date.today().isoformat()

    metrics = (
        "views,likes,dislikes,comments,shares,"
        "estimatedMinutesWatched,averageViewDuration,"
        "averageViewPercentage,subscribersGained,subscribersLost,"
        "annotationClickThroughRate,annotationCloseRate,"
        "cardClicks,cardTeaserClicks,cardImpressions,cardTeaserImpressions,"
    )

    response = analytics_api.reports().query(
        ids='channel==MINE',
        startDate=start_date,
        endDate=end_date,
        metrics=metrics,
        dimensions="day",
    ).execute()

    rows = response.get('rows', [])
    col_headers = [col.get('name', []) for col in response['columnHeaders']]
    
    records = []
    for row in rows:
        record = dict(zip(col_headers, row))
        records.append(record)

    day_metrics = pd.DataFrame(records)

    return day_metrics


if __name__ == "__main__":

    # Load from .env file
    load_dotenv()
    api_key = os.getenv('YI_API_KEY')
    client_id = os.environ["YOUTUBE_CLIENT_ID"]
    client_secret = os.environ["YOUTUBE_CLIENT_SECRET"]
    refresh_token = os.environ["YOUTUBE_REFRESH_TOKEN"]

    analytics_api = connect_yt_analytics_api(api_key, refresh_token, client_id, client_secret)

    day_metrics = fetch_day_data(analytics_api)



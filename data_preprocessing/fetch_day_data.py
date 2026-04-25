from googleapiclient.discovery import build 
from dotenv import load_dotenv
import os
from isodate import parse_duration
import pandas as pd
from datetime import date, timedelta
import json
from helper_functions import connect_yt_analytics_api, insert_records_to_postgres
import psycopg2
from psycopg2 import sql


def table_max_date(pg_table_name):

    dbl_url = os.environ['DBL_URL']
    
    conn = psycopg2.connect(dbl_url)
    cur = conn.cursor()

    max_date_query = sql.SQL(f'SELECT MAX(date) FROM stage.{pg_table_name}')
    
    cur.execute(max_date_query)
    max_date = cur.fetchone()[0]        

    return max_date


def fetch_day_full_data(analytics_api, action):

    '''
    Fetches daily Youtube channel analytics metrics and returns a Pandas dataframe

    This function: 
    1. Queries the Youtube Analytics API for the channel's daily metrics
    2. Retrieves a set of daily metrics e.g. views, likes, comments, shares etc 
    3. Converts the API response into a DF with one row per day, storing the full day's merics as a JSON string

    Args: 
        analytics_api: Authenticated Youtube Analytics API

    Returns:
        A pandas DF with columns:
            date: Date of the metrics
            date_metrics: JSON string containing all daily metrics
    '''

    if action not in ('truncate', 'append'):
        raise ValueError('action must be truncate or append')
    
    if action == 'truncate':
        start_date = '2022-09-16'  # YouTube's earliest possible date
        end_date = date.today().isoformat()

    else: 
        start_date = table_max_date('sc_yt_day_data') - timedelta(days=7)
        start_date = start_date.isoformat()
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
    
    records = {}
    for row in rows:
        record = dict(zip(col_headers, row))
        key = record['day']
        records[key] = record

    day_metrics = pd.DataFrame(
        [(key, json.dumps(value)) for key, value in records.items()], 
        columns=['date', 'date_metrics']
    )

    return day_metrics

import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import math
import os
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def run_sql_query(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(sql.SQL(query).as_string(conn), conn)
        return df 
    finally: 
        conn.close()

query = """
    with min_viewers as (
        select
            video_id
            , livestream_position 
            , peak_concurrent_viewers 
            , average_concurrent_viewers 
            , row_number() over(partition by video_id, livestream_position order by load_ts desc) as rn
            
        from rdv.social_content_sat_min_viewers 
    )

    , video_details as (
        select distinct 
            video_id
            , video_duration_sec
        
        from rdv.social_content_sat_details 
    )

    , base as (
        select 
            mv.video_id
            , mv.livestream_position 
            , mv.peak_concurrent_viewers 
            , mv.average_concurrent_viewers 
        
        from min_viewers as mv 
        
        left join video_details as vd 
            on mv.video_id = vd.video_id
        
        where 1=1 
            and mv.rn = 1
            and video_duration_sec < 5400
    )

    select *
    from base
"""

df_orig = run_sql_query(query)

df_cond = df_orig.sort_values(['video_id', 'livestream_position'])
df_cond['diff'] = df_cond.groupby('video_id')['average_concurrent_viewers'].diff()

df_cond['view_movement'] = np.select(
    [(df_cond['average_concurrent_viewers'] > 1) & (df_cond['diff'] > 0), (df_cond['average_concurrent_viewers'] > 0) & (df_cond['diff'] < 0), (df_cond['average_concurrent_viewers'] > 1) & (df_cond['diff'] == 0)],
    ['spike', 'drop', 'retain'],
    default='NULL'
) 

null_vids = df_cond.groupby('video_id')['average_concurrent_viewers'].value_counts(normalize=True).reset_index()
null_vids = null_vids[null_vids['proportion'] == 1]['video_id'].unique() # videos with no average viewers per min, most likely because of error 

df_cond = df_cond[~df_cond['video_id'].isin(null_vids)]

df_check = df_cond.groupby('video_id')['view_movement'].value_counts(normalize=True).reset_index()
df_check[df_check['view_movement'] == 'retain'].sort_values('proportion')

## SCENARIO 1: Find videos with the longest runs of RETAIN and SPIKE and understand themes within those

df_cond_s1 = df_cond
df_cond_s1['view_movement'] = df_cond_s1['view_movement'].replace('drop', 'NULL')

df_cond_s1_all = []
for i in df_cond['video_id'].unique():
    df_cond_s1 = df_cond[df_cond['video_id'] == i]

    movement_scenarios = []
    movement_count = 0
    for change in df_cond_s1['view_movement']:
        if change == 'NULL':
            movement_count = 0
            movement_scenarios.append(movement_count)
        else:
            movement_count += 1 
            movement_scenarios.append(movement_count)

    df_cond_s1['scenario_1_run'] = movement_scenarios
    df_cond_s1_all.append(df_cond_s1) 

## UPDATE: For each video id, create a column called scenario_1_run, where it applies a counter to which 'Retain' or 'Spike' is in 'view_movement' consecutively. 
# As soon as NULL appears (i.e. drop or actual NULL from above conditions) then put 0 and start counter again
df_cond_s1_all = pd.concat(df_cond_s1_all, ignore_index=True)

# Find the highest number of runs for each video. Group them. Check any commenalities e.g. video timestamps, speakers, topic types etc. 

df_cond_s1_sum = ( 
    df_cond_s1_all
    .groupby('video_id')
    .agg(
        max_run=('scenario_1_run', 'max'),
        total_rows=('scenario_1_run', 'size'),
        avg_viewers_per_min=('average_concurrent_viewers', 'mean')
    )
    .reset_index()
)

df_cond_s1_sum['retain_pct'] = df_cond_s1_sum['max_run'] / df_cond_s1_sum['total_rows']
df_cond_s1_sum = df_cond_s1_sum[df_cond_s1_sum['total_rows'] > 60] # Only look at videos where video length is more than 60 mins

median_viewers = df_cond_s1_sum['avg_viewers_per_min'].median()
median_retain_pct = df_cond_s1_sum['retain_pct'].median()

df_cond_s1_sum['segments'] = np.select(
    [(df_cond_s1_sum['avg_viewers_per_min'] > median_viewers) & (df_cond_s1_sum['retain_pct'] > median_retain_pct),
     (df_cond_s1_sum['avg_viewers_per_min'] > median_viewers) & (df_cond_s1_sum['retain_pct'] < median_retain_pct),
     (df_cond_s1_sum['avg_viewers_per_min'] < median_viewers) & (df_cond_s1_sum['retain_pct'] < median_retain_pct),
     (df_cond_s1_sum['avg_viewers_per_min'] < median_viewers) & (df_cond_s1_sum['retain_pct'] > median_retain_pct),
    ],

    ['high_viewers_high_ret', 
     'high_viewers_low_ret', 
     'low_viewers_low_ret', 
     'low_viewers_high_ret'],

    default='NULL'
) 

video_query = """
    select distinct
        video_id
        , video_title_raw
        , video_published_at
        
    from rdv.social_content_sat_details 
"""

df_video_info = run_sql_query(video_query)

df_cond_s1_sum = df_cond_s1_sum.merge(df_video_info, on='video_id', how='left')

# url = 'https://www.youtube.com/watch?v=JFkCdCna7zs' ## YOUTUBE LIVESTREAMS TODO


all_vids = []
for vid in df_cond_s1_sum['video_id'].unique():
    all_vids.append(vid)

folder = 'data_dashboard/video_transcripts'

file_names = [
    f for f in os.listdir(folder)
]

file_names = [
    f.replace('.txt', '') for f in file_names 
]

df_file_names = pd.DataFrame(file_names, columns=['video_id'])
df_file_names['downloaded'] = 'Y'
df_all_vids = pd.DataFrame(all_vids, columns=['video_id'])
df_all_vids = df_all_vids.merge(df_file_names, on='video_id', how='left')

for id in df_all_vids[df_all_vids['downloaded'].isna()]['video_id']:
    print(f'https://www.youtube.com/watch?v={id}')




'''
Column headers: 
- max_run - number of consecutive minutes where there was a spike or retain for average viewers
- total_rows - how many minutes altogether
- avg_viewers_per_min - average viewers per min
- retain_pct - max_run / total_rows - percentage of video covered by the max run
'''


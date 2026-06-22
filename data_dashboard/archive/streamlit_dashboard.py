import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from sklearn.cluster import KMeans
import streamlit as st


## may change this to total subs as big number (the same)
## small number as pct difference compared to prev week
## chart as number of subs of last few weeks

## this part below wondering if it should be in the breakdown 

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
    select * from rdv.social_content_sat_min_viewers
    """

df = run_sql_query(query)

## Cluster streams into groups - i.e. consistent viewers, early drops, peaks etc 

## A. Streams have different lengths. Resample the mins so it's in percentages instead and then comparable. E.g. Average viewers of first 1% of video
df_resample = df.copy()
df_resample['livestream_min'] = (df_resample['livestream_position'] / 60)
df_resample['video_livestream_max_length'] = df_resample.groupby('video_id')['livestream_min'].transform('max')
df_resample['stream_duration_pct'] = (df_resample['livestream_min'] / df_resample['video_livestream_max_length']) * 100

df_resample['average_concurrent_viewers'] = pd.to_numeric(df_resample['average_concurrent_viewers'], errors='coerce')
df_resample['peak_concurrent_viewers'] = pd.to_numeric(df_resample['peak_concurrent_viewers'], errors='coerce')

df_resample['stream_duration_pct_bucket'] = np.floor(df_resample['stream_duration_pct'])
df_resample = df_resample.groupby(['video_id', 'stream_duration_pct_bucket']).agg({'average_concurrent_viewers': 'mean', 'peak_concurrent_viewers': 'max'}).reset_index()

df_pivot = df_resample.pivot_table(
    index='video_id', 
    columns='stream_duration_pct_bucket',
    values='average_concurrent_viewers'
).reset_index()

# Since lengths of videos are varying, we will not find average viewers for every pct point. So interpolate the missing pct for average viewers, with neighbouring average
pct_cols = df_pivot.columns.drop('video_id')
df_pivot_filled = df_pivot.copy()
df_pivot_filled[pct_cols] = df_pivot[pct_cols].interpolate(axis=1)

# Filter video streams to livestreams

video_query = """
    select * from pl.pl_dim_video
"""

df_video = run_sql_query(video_query)

df_pivot_filled = df_pivot_filled.merge(df_video[['video_id', 'video_title', 'video_type']], on='video_id', how='left')
df_pivot_filled = df_pivot_filled[df_pivot_filled['video_type'] == 'Livestream']

# '''
# Understand viewership clustering i.e. do we see the following: 
# - Consistent viewership
# - Early spikes / drops
# - Mid spikes / drops
# - Low spikes / drops 
# '''


# Step 1: Cluster streams and create medians. Only present the medians with variability bands - DONE
# Step 2: 'Explore a cluster' give participants the option to select a cluster which will turn the big cluster into many thin lines. Next to each one show stats e.g. peak min, peak viewers etc of the cluster as a whole
# Step 3: Then give viewers the option to click a stream and then show details e.g. stream name, speaker etc - think about 'what about this gathering explains why it behaved this way'
# Step 4: Think about how to present newly finished streams - think about: 'this weeks gathering most closely resembled: Slow build, strong finish cluster'
# Step 5: Group the characteristics of each cluster to advise on new streams - e.g. 'Gatherings with these characteristics tend to produce this attention pattern'


df_pivot_filled = df_pivot_filled.drop(columns='video_type')

df_pivot_filled['avg_viewers'] = df_pivot_filled.iloc[:, 1:-1].mean(axis=1)
df_filtered = df_pivot_filled[df_pivot_filled['avg_viewers'] > 10]

x = df_filtered.iloc[:, 1:-2]
x = x.astype(float)
x_norm = (x-x.min(axis=1).values[:, None]) / (x.max(axis=1).values[:, None] - x.min(axis=1).values[:, None])

k = 4

kmeans = KMeans(
    n_clusters=k,
    random_state=42,
    n_init=20
)

clusters = kmeans.fit_predict(x_norm)

df_filtered['cluster'] = clusters

df_filtered['cluster'] = df_filtered['cluster'].map({0: 'Mid-Peak Drop', 1: 'Mid-Peak Stable', 2: 'Irregular', 3: 'Early-Rise Stable'})

def cluster_median_chart_layout(df, cluster, title):

    df_cluster = df[df['cluster'] == cluster]
    df_cluster_median = df_cluster.iloc[:, 1:-3].median().reset_index(name='average_viewers').rename(columns={'index':'pct_video'})

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_cluster_median['pct_video'], 
            y=df_cluster_median['average_viewers'],
            mode='lines',
        )
    )

    fig.update_layout(
        title=title, 
        height=350,
        xaxis=dict(title='X Axis'),
        yaxis=dict(title='Y Axis'),
        template='plotly_white'
    )

    return fig


def cluster_all_chart_layout(df, cluster, title):

    df_cluster = df[df['cluster'] == cluster]
    # df_cluster = df_cluster.iloc[:, 1:-3]

    x_cols = df_cluster.iloc[:, 1:-3].columns

    fig = go.Figure()

    for _, row in df_cluster.iterrows():
        fig.add_trace(
            go.Scatter(
                x=x_cols,
                y=row[x_cols],
                mode='lines',
                name=row['video_title'],
                line=dict(width=0.5)
            )
        )
    
    fig.update_layout(
        title=title, 
        height=350,
        xaxis=dict(title='X Axis'),
        yaxis=dict(title='Y Axis'),
        template='plotly_white'
    )

    return fig


st.set_page_config(layout="wide")

with st.sidebar:
    cluster_options = st.radio(
        'Choose a cluser', 
        ['No Cluster', 'Mid-Peak Drop', 'Mid-Peak Stable', 'Irregular', 'Early-Rise Stable']
    )

row1 = st.columns(2)
row2 = st.columns(2)


## TODO: Clicking on each line gives me the metadata for each gathering. Get Transcript for each video. Follow this for now:

## Introduction: 00:00:05 - 00:02:41
## Quran Recitation / Nasheed: 00:02:41 - 00:10:43
## Main Talk: 00:10:43 - 00:55:54
## Q&A Session: 00:55:54 - 01:02:02
## Dhikr (Remembrance): 01:02:02 - 01:26:15
## Final Announcements: 01:26:15 - End

## Also include number of times where there are 5+ second gaps - indicate media issues 


with row1[0]: 
    if cluster_options == 'Mid-Peak Drop':
        st.plotly_chart(cluster_all_chart_layout(df_filtered, 'Mid-Peak Drop', 'Mid-Peak Drop'))
    else: 
        st.plotly_chart(cluster_median_chart_layout(df_filtered, 'Mid-Peak Drop', 'Mid-Peak Drop'))

with row1[1]: 
    if cluster_options == 'Mid-Peak Stable':
        st.plotly_chart(cluster_all_chart_layout(df_filtered, 'Mid-Peak Stable', 'Mid-Peak Stable'))
    else:
        st.plotly_chart(cluster_median_chart_layout(df_filtered, 'Mid-Peak Stable', 'Mid-Peak Stable'))

with row2[0]: 
    if cluster_options == 'Irregular':
        st.plotly_chart(cluster_all_chart_layout(df_filtered, 'Irregular', 'Irregular'))
    else:
        st.plotly_chart(cluster_median_chart_layout(df_filtered, 'Irregular', 'Irregular'))

with row2[1]: 
    if cluster_options == 'Early-Rise Stable':
        st.plotly_chart(cluster_all_chart_layout(df_filtered, 'Early-Rise Stable', 'Early-Rise Stable'))
    else:
        st.plotly_chart(cluster_median_chart_layout(df_filtered, 'Early-Rise Stable', 'Early-Rise Stable'))


# # Streamlit dashboard of the viewership
# df_pivot_filled = df_pivot_filled.drop(columns='video_type')

# st.set_page_config(layout="wide")

# with st.container(width='stretch', height='stretch'):

#     x_cols = df_pivot_filled.columns[1:]

#     df_pivot_filled['avg_viewers'] = df_pivot_filled.iloc[:, 1:].mean(axis=1)

#     df_filtered = df_pivot_filled[df_pivot_filled['avg_viewers'] > 10]

#     default_vid = df_filtered.sort_values(by='avg_viewers', ascending=False).iloc[2]

#     video_options = df_filtered['video_id'].astype(str).unique().tolist()
#     selected_videos = st.multiselect(
#         'Select Videos', 
#         options=video_options, 
#         default=default_vid['video_id']
#     )
    
#     fig = go.Figure()

#     for _, row in df_filtered[df_filtered['video_id'].isin(selected_videos)].iterrows():
#         fig.add_trace(
#             go.Scatter(
#                 x=x_cols,
#                 y=row[x_cols],
#                 mode='lines+markers',
#                 name=row['video_id']
#             )
#         )
    
#     st.plotly_chart(fig, use_container_width=True)








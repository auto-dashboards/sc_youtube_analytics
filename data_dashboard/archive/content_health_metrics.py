import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from prophet import Prophet
from datetime import datetime
import streamlit as st
import plotly.express as px

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


def video_health_metrics():

    query = """
        select *
        from pl.pl_dim_video 
        """
    
    df = run_sql_query(query)

    return df 

df = video_health_metrics()

df = df[df['video_type'].isin(['Livestream', 'Luton Livestream'])]

df['avg_view_duration_mins'] = round(df['video_estimated_minutes_watched'] / df['video_views'], 1) # On average, minutes watched per view
df['avg_pct_video_watched'] = (df['video_estimated_minutes_watched'] * 60 / (df['video_views'] * df['video_duration_sec'])) * 100 # On average, pct of video watched
df['engagement_metric'] = round(((df['video_likes'] + df['video_comments']) / df['video_views']) * 100, 1)

st.set_page_config(layout="wide")

fig = px.scatter(
    df, 
    x='avg_view_duration_mins', 
    y='engagement_metric', 
    size_max=8,
    hover_data={
        'video_speaker':True,
        'video_title':True,
        'video_published_at':True, 
        'video_likes':True, 
        'video_comments': True,
        'video_views':True,
    },

    labels={
        'avg_view_duration_mins': '⏱️ Avg. View Duration (mins) ', 
        'engagement_metric': '📊 Engagement Score ',
        'video_speaker': '🎙️ Speaker ',
        'video_title': '📌 Video Title ',
        'video_published_at': '📅 Published ',
        'video_likes': '👍 Likes ',
        'video_comments': '💬 Comments ',
        'video_views': '👁️ Views ',
    },
)

fig.update_traces(
    textposition="top center", 
    marker=dict(
        size=6, 
        line=dict(
            width=0.3, 
            color="black"
        )
    )
)

fig.update_layout(
    title="Quadrant Analysis of Video Performance",
    xaxis_title='Avg. View Duration (mins)',
    yaxis_title='Engagement Score',
    height=800, 
    width=1200, 
    showlegend=False,
)

fig.update_xaxes(range=[0, 18], showgrid=False)
fig.update_yaxes(range=[0, 18], showgrid=False)

v_line = round(df['avg_view_duration_mins'].max() / 2)
h_line = round(df['engagement_metric'].max() / 2)

# Define slider width and hide in an expander
with st.sidebar:
    # User quadrant guidelines 
    eng_metric_cutoff = st.number_input(
        "Engagement Metric Cutoff",
        min_value=0.0,
        max_value=float(round(df['engagement_metric'].max())) + 1,
        value=float(h_line),
        step=0.5
    )
    avg_duration_cutoff = st.number_input(
        "Average Duration Per View Cutoff",
        min_value=0.0,
        max_value=float(round(df['avg_view_duration_mins'].max())) + 1,
        value=float(v_line), 
        step=0.5
    )

# User quadrant guidelines
fig.add_vline(x=avg_duration_cutoff, line_width=0.5, line_color="#AAAAAA")
fig.add_hline(y=eng_metric_cutoff, line_width=0.5, line_color="#AAAAAA")

quad_config = {
    'top_right': {
        'filter': lambda d: d[(d['avg_view_duration_mins'] >=  avg_duration_cutoff) & (d['engagement_metric'] >= eng_metric_cutoff)], 
        'rect': {
            'x0': avg_duration_cutoff, 
            'x1': 18, 
            'y0': eng_metric_cutoff, 
            'y1': 18, 
            'fillcolor_default': '#A9DFBF', 
            'fillcolor_highlight': '#58D68D',
            'line_highlight': '#27AE60',
            'annotation_y': 17.5
        }, 
        'quadrant_title': 'Top Performers', 
        'quadrant_desc': 'Content that retains viewers and drives strong interaction.',
        'quadrant_action': 'Identify what made these successful and replicate the format, structure and packaging.'
    }, 

    'top_left': {
        'filter': lambda d: d[(d['avg_view_duration_mins'] <  avg_duration_cutoff) & (d['engagement_metric'] > eng_metric_cutoff)], 
        'rect': {
            'x0': 0,
            'x1': avg_duration_cutoff, 
            'y0': eng_metric_cutoff, 
            'y1': 18, 
            'fillcolor_default': '#F5B7B1', 
            'fillcolor_highlight': '#F1948A', 
            'line_highlight': '#C0392B',
            'annotation_y': 17.5
        }, 
        'quadrant_title': 'High Interaction',
        'quadrant_desc': 'Viewers interact actively but don’t watch for long.',
        'quadrant_action': 'Review pacing, structure, and introductions to improve retention without reducing engagement.'
    }, 

    'btm_left': {
        'filter': lambda d: d[(d['avg_view_duration_mins'] < avg_duration_cutoff) & (d['engagement_metric'] < eng_metric_cutoff)], 
        'rect': {
            'x0': 0, 
            'x1': avg_duration_cutoff, 
            'y0': eng_metric_cutoff, 
            'y1': 0, 
            'fillcolor_default': '#F2F3F4', 
            'fillcolor_highlight': '#BDC3C7', 
            'line_highlight': '#95A5A6',
            'annotation_y': eng_metric_cutoff - 0.5
        }, 
        'quadrant_title': 'Low Performers',
        'quadrant_desc': 'Content underperforms on both retention and interaction.',
        'quadrant_action': 'Audit title/thumbnail relevance, content clarity, and topic fit; consider testing new formats.'
    },

    'btm_right': {
        'filter': lambda d: d[(d['avg_view_duration_mins'] >=  avg_duration_cutoff) & (d['engagement_metric'] <  eng_metric_cutoff)], 
        'rect': {
            'x0': avg_duration_cutoff, 
            'x1': 18, 
            'y0': eng_metric_cutoff, 
            'y1': 0, 
            'fillcolor_default': '#FCF3CF', 
            'fillcolor_highlight': '#F7DC6F', 
            'line_highlight': '#F1C40F',
            'annotation_y': eng_metric_cutoff - 0.5
        },
        'quadrant_title': 'Strong Retention',
        'quadrant_desc': 'Videos hold attention but generate limited interaction.',
        'quadrant_action': 'Improve calls to action, thumbnail/title clarity, or prompt discussion to lift engagement.'
    }
}

def quadrant_insight(df, eng_metric_cutoff, avg_duration_cutoff, quadrant_config, quadrant):

    if quad_desc_radio == quadrant_config[quadrant]['quadrant_title']:

        fillcolor = quadrant_config[quadrant]['rect']['fillcolor_highlight']

        for alpha in [0.15, 0.1, 0.05]:
            fig.add_shape(
                type='rect', 
                x0=quadrant_config[quadrant]['rect']['x0'], 
                x1=quadrant_config[quadrant]['rect']['x1'],
                y0=quadrant_config[quadrant]['rect']['y0'],
                y1=quadrant_config[quadrant]['rect']['y1'],
                fillcolor=fillcolor,
                line=dict(
                    color=quadrant_config[quadrant]['rect']['line_highlight'],
                    width=4
                ),
                opacity=alpha, 
                # line_width=0
            )

            fig.add_annotation(
                x=(quadrant_config[quadrant]['rect']['x0'] + quadrant_config[quadrant]['rect']['x1']) / 2,
                y=quadrant_config[quadrant]['rect']['annotation_y'], 
                text=quadrant_config[quadrant]['quadrant_title'], 
                showarrow=False, 
                font=dict(size=14, color="black"),
                align="center",
                bgcolor="rgba(255,255,255,0.1)", 
            )
        
        st.sidebar.markdown(f"#### Quadrant Summary")

        st.sidebar.markdown(
            f"""
            <div style="
                padding: 15px; 
                margin-top: 15px; 
                border: 1px solid #ddd; 
                border-radius: 10px;
                background-color: #f9f9f9;
                font-size: 14px;
                line-height: 1.5;
            ">
                <h4 style='margin-bottom: 1px;'>{quadrant_config[quadrant]['quadrant_desc']}</h4>
                <p style='margin-bottom: 5px;'>{quadrant_config[quadrant]['quadrant_action']}</p>
                <hr style='margin: 10px 0;'>
            </div>
            """,
            unsafe_allow_html=True
        )

quadrants = ['top_right', 'top_left', 'btm_left', 'btm_right']

quad_desc_radio = st.sidebar.radio(label='Choose Quadrant: ', options=['None Selected'] + [quad_config[q]['quadrant_title'] for q in quadrants])

for q in quadrants:
    quadrant_insight(df, eng_metric_cutoff, avg_duration_cutoff, quad_config, q)

col_left, col_center, col_right = st.columns([0.5, 3, 0.5])

with col_center:
    st.plotly_chart(fig, use_container_width=False)
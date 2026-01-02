import streamlit as st
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.express as px
from datetime import datetime, date, timedelta
import plotly.graph_objects as go
from prophet import Prophet

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
select 
    metric_date
    , platform
    , subscribers_gained
    , subscribers_lost 
    , (subscribers_gained - subscribers_lost) as net_subscribers

from pl.pl_fct_channel_metrics_daily 
"""

df = run_sql_query(query)

# --- Step 1: Trend analysis of actual subs vs predicted ---

# --- Step 1.1: Roll up data at a week level ---
df['metric_date'] = pd.to_datetime(df['metric_date'])
df['week_commencing_dt'] = df['metric_date'] - pd.to_timedelta(df['metric_date'].dt.weekday, unit='D')
df_week = df.groupby(['week_commencing_dt', 'platform'])['net_subscribers'].sum().reset_index()

# --- Step 1.2: Use prophet to predict data points ---

df_prophet = df_week[['week_commencing_dt', 'net_subscribers']].rename(
    columns={'week_commencing_dt': 'ds', 'net_subscribers': 'y'}
)

m = Prophet()
m.fit(df_prophet)

future_dates = pd.date_range(
    start=df_prophet['ds'].max() + pd.Timedelta(weeks=1),
    end=pd.to_datetime('2026-11-01'),
    freq='W'
)

future = pd.DataFrame({'ds': future_dates})
future_all = pd.concat([df_prophet[['ds']], future], ignore_index=True)
forecast = m.predict(future_all)

forecast['ds'] = pd.to_datetime(forecast['ds'])
df_prophet['ds'] = pd.to_datetime(df_prophet['ds'])
df_all = forecast.merge(df_prophet, how='left', on='ds')[['ds', 'y', 'yhat']]

df_all['actual_running_sub'] = df_all.sort_values('ds')['y'].cumsum()
df_all['predicted_running_sub'] = df_all.sort_values('ds')['yhat'].cumsum()

left_col, right_col = st.columns([2, 3])

with left_col:
    with st.container(border=True):
        st.metric(label='Subscribers', value='--', delta='--', delta_color='off')
        st.metric(label='Subscribers', value='--', delta='--', delta_color='off')

with right_col:
    row1, row2 = st.columns(2)

    row1.metric(label='Likes', value='--', delta='--', delta_color='off', border=True)
    row2.metric(label='Comments', value='--', delta='--', delta_color='off', border=True)

    row1.metric(label='Shares', value='--', delta='--', delta_color='off', border=True)
    row2.metric(label='Saves', value='--', delta='--', delta_color='off', border=True)


# # --- Step 1.2: Create plot for actual running subs vs preds ---
# df_all['actual_running_sub'] = df_all.sort_values('ds')['y'].cumsum()
# df_all['predicted_running_sub'] = df_all.sort_values('ds')['yhat'].cumsum()

# fig = go.Figure()

# fig.add_trace(go.Scatter(
#     x=df_all['ds'],
#     y=df_all['actual_running_sub'],
#     mode='lines',
#     name='Actual',
#     line=dict(dash='solid', color='blue')
# ))

# fig.add_trace(go.Scatter(
#     x=df_all[df_all['actual_running_sub'].isna()]['ds'],
#     y=df_all[df_all['actual_running_sub'].isna()]['predicted_running_sub'],
#     mode='lines',
#     name='Predicted',
#     line=dict(dash='dot', color='red')
# ))

# # --- Step 1.6: Update layout ---
# fig.update_layout(
#     title='Title',
#     xaxis=dict(range=[df_all['ds'].min(), df_all['ds'].max()], showgrid=False),
#     yaxis=dict(showgrid=False),
#     xaxis_title="xaxis",
#     yaxis_title="yaxis",
#     height=600, width=800, 
#     showlegend=False,
# )

# fig.add_shape(
#     type="line",
#     x0=0,
#     x1='2026-05-01', 
#     y0=1500,
#     y1=1500,
#     line=dict(color="lightgrey", width=0.5, dash="dash")
# )

# fig.add_shape(
#     type="line",
#     x0="2026-05-01",
#     x1="2026-05-01",  
#     y0=0,             
#     y1=1500,  
#     line=dict(color="lightgrey", width=0.5, dash="dash")
# )

# fig.add_annotation(
#     x='2025-11-01',
#     y=1500 + 50,
#     text='Target: 1500 subs by May 2026',
#     showarrow=False,
#     xanchor='right',
#     font=dict(color='lightgrey')
# )

# fig.add_shape(
#     type="line",
#     x0=0,
#     x1='2026-11-01', 
#     y0=2000,
#     y1=2000,
#     line=dict(color="lightgrey", width=0.5, dash="dash")
# )

# fig.add_shape(
#     type="line",
#     x0='2026-11-01',
#     x1='2026-11-01', 
#     y0=0,
#     y1=2000,
#     line=dict(color="lightgrey", width=0.5, dash="dash")
# )

# fig.add_annotation(
#     x='2025-11-01',
#     y=2000 + 50,
#     text='Target: 2000 subs by Nov 2026',
#     showarrow=False,
#     xanchor='right',
#     font=dict(color='lightgrey')
# )

# st.plotly_chart(fig, width='stretch')

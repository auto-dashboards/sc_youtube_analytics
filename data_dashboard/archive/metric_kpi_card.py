import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from prophet import Prophet
from datetime import datetime

## Total metric as main number 
## Show %difference compared to expected value from lin reg on the last 12 weeks
## Total metric per month - chart the last 12 months

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


def make_forecast_metric(df, metric):
    '''
    Return a dataframe which predicts views based on the last completed 12 weeks. 
    Excluding the latest week as this won't be completed and the week before as we'll use this to compare. Actuals vs predicted

    metric: Metric we want the latest completed 12 weeks for. If you provide a list i.e. ['estimated_watch_minutes, 'views_count'] it'll provide values for both. 
    Necessary incase we want to create another metric from it

    df: Dataframe with date and views

    Return: Latest completed 12 weeks. With latest completed week and views and predicted views
    '''

    # --- Step 1: Roll up data at a week level ---
    df['metric_date'] = pd.to_datetime(df['metric_date'])
    df['week_commencing_dt'] = df['metric_date'] - pd.to_timedelta(df['metric_date'].dt.weekday, unit='D')

    if isinstance(metric, str):
        metric = [metric]

    agg_dict = {m: 'sum' for m in metric}
    df_week = df.groupby(['week_commencing_dt', 'platform']).agg(agg_dict).reset_index()

    # --- Step 2: Fit a Linear Regression on the last 12 weeks, excluding last 2 weeks. Use the latest week to compare with predicted ---
    df_week_lg = df_week.sort_values('week_commencing_dt')[-14:-1]

    df_week_lg['point_type'] = np.where(
        df_week_lg['week_commencing_dt'] == df_week_lg['week_commencing_dt'].max(), 
        'COMPARISON - ACTUAL', 
        'ACTUAL'
    )

    df_week_lg_actual = df_week_lg[df_week_lg['point_type'] == 'ACTUAL']

    x = df_week_lg_actual['week_commencing_dt'].map(datetime.toordinal)
    y = df_week_lg_actual[metric]

    slope, intercept = np.polyfit(x, y, 1)

    # --- Step 3: Predict the next point using it ---
    next_date = df_week_lg_actual['week_commencing_dt'].max() + pd.Timedelta(weeks=1)
    next_date_ord = next_date.toordinal()

    next_date_pred = (slope * next_date_ord) + intercept
    next_date_pred = [round(x) for x in next_date_pred]

    # --- Step 4: Create a DF based on next date and next date prediction. Then combine with actuals ---
    metrics_est = dict(zip(metric, next_date_pred))
    metrics_est['week_commencing_dt'] = next_date
    metrics_est['platform'] = 'YOUTUBE'
    metrics_est['point_type'] = 'COMPARISON - PREDICTED'

    df_pred_lg = pd.DataFrame([metrics_est])

    df_all = pd.concat([df_week_lg, df_pred_lg]).reset_index(drop=True)

    if len(metric) == 2: 
        df_all['derived_metric'] = df_all[metric[0]] / df_all[metric[1]]
    else: 
        pass

    return df_all


def make_metric_card(df, num_months, metric):
    '''
    Return a plotly figure which will be used as a spark bar chart in main file for the views metric card. Last N Month. Excluding latest Month

    df: Full DF
    num_weeks: Latest number of months
    '''

    df['metric_date'] = pd.to_datetime(df['metric_date'])
    df['month_start_dt'] = df['metric_date'].dt.to_period('M').dt.to_timestamp()

    if isinstance(metric, str):
        metric = [metric]

    agg_dict = {m: 'sum' for m in metric}
    df_month = df.groupby(['month_start_dt', 'platform']).agg(agg_dict).reset_index()

    df_month_latest = df_month[-num_months-1:-1]

    if len(metric) == 2: 
        df_month_latest['derived_metric'] = df_month_latest[metric[0]] / df_month_latest[metric[1]]
        metric = 'derived_metric'

    else: 
        metric = metric[0]

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_month_latest['month_start_dt'], 
            y=df_month_latest[metric], 
            mode='lines+markers',
            line=dict(color='blue'),
            hoverinfo='y',
            marker=dict(
                size=4,
                color=['blue' if i == 0 or i == len(df_month_latest['month_start_dt'])-1 else 'rgba(0,0,0,0)' for i in range(len(df_month_latest['month_start_dt']))]
            )
        )
    )

    df_month_latest_sorted = df_month_latest.sort_values(by='month_start_dt')
    first_x = df_month_latest_sorted['month_start_dt'].iloc[0]
    first_y = round(df_month_latest_sorted[metric].iloc[0])
    last_x = df_month_latest_sorted['month_start_dt'].iloc[-1]
    last_y = round(df_month_latest_sorted[metric].iloc[-1])

    fig.add_annotation(
        x=first_x, 
        y=first_y, 
        text=f'{first_y}',
        showarrow=False,
        xanchor='left',
        yanchor='top',
        font=dict(size=10)
    )

    fig.add_annotation(
        x=last_x - pd.Timedelta(days=25), 
        y=last_y, 
        text=f'{last_y}',
        showarrow=False,
        xanchor='left',
        yanchor='top',
        font=dict(size=10),
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=12),  
        xaxis=dict(
            title=dict(
                text='Last 12 Months', 
                font=dict(size=10)
            ),
            showticklabels=False
        ),
        yaxis=dict(visible=False),  
        height=50,   
        showlegend=False                           
    )

    return fig


def metric_forecast_stats(metric):

    query = """
        select 
            metric_date
            , platform
            , (subscribers_gained - subscribers_lost) as net_subscribers
            , views_count
            , likes_count
            , dislikes_count
            , comments_count
            , shares_count
            , estimated_watch_minutes

        from pl.pl_fct_channel_metrics_daily 
        """
    
    df = run_sql_query(query)
    df_fc = make_forecast_metric(df, metric)

    if len(metric) == 2: 
        metric_total = df[metric[0]].sum() / df[metric[1]].sum()
        metric_total_format = f'{metric_total:,.2f}'
        metric_deviation = 'derived_metric'

    else:
        metric_total = df[metric].sum()
        metric_total_int = int(metric_total)
        metric_total_format = f'{metric_total_int:,}'
        metric_deviation = metric

    latest_week_actual = df_fc[df_fc['point_type'] == 'COMPARISON - ACTUAL'][metric_deviation].iloc[0]
    latest_week_pred = df_fc[df_fc['point_type'] == 'COMPARISON - PREDICTED'][metric_deviation].iloc[0]
    latest_week_dt = df_fc['week_commencing_dt'].max().strftime('%d %b %Y')

    def pct_diff(pred, actual):
        if pred == 0:
            return 0
        result = (actual / pred) - 1
        return 0 if np.isinf(result) or np.isnan(result) else result

    latest_week_deviation = str(round((pct_diff(latest_week_pred, latest_week_actual)) * 100)) + '%'
    latest_week_dev_summary = 'WC ' + latest_week_dt + ': ' + latest_week_deviation + ' vs forecast'

    fig = make_metric_card(df, 12, metric)

    return metric_total_format, latest_week_dev_summary, fig
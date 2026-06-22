import psycopg2
from psycopg2 import sql
import pandas as pd
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from prophet import Prophet

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


def make_forecast(df, final_pred_date):
    '''
    Use prophet to project points up until pred_date 

    df: Dataframe 
    final_pred_date: Final projection date

    Return: 
    ds: Week commencing date 
    y: Actual value
    yhat: Predicted value
    actual_running_sub: Running sum based on actual values 
    predicted_running_sub: Running sum based on predicted values
    
    '''
    # --- Step 1: Roll up data at a week level ---
    df['metric_date'] = pd.to_datetime(df['metric_date'])
    df['week_commencing_dt'] = df['metric_date'] - pd.to_timedelta(df['metric_date'].dt.weekday, unit='D')
    df_week = df.groupby(['week_commencing_dt', 'platform'])['net_subscribers'].sum().reset_index()

    # --- Step 2: Use prophet to predict data points. Only predict up till X point ---
    df_prophet = df_week[['week_commencing_dt', 'net_subscribers']].rename(
        columns={'week_commencing_dt': 'ds', 'net_subscribers': 'y'}
    )
    m = Prophet()
    m.fit(df_prophet)

    future_dates = pd.date_range(
        start=df_prophet['ds'].max() + pd.Timedelta(weeks=1),
        end=pd.to_datetime(final_pred_date),
        freq='W'
    )

    future = pd.DataFrame({'ds': future_dates})
    future_all = pd.concat([df_prophet[['ds']], future], ignore_index=True)
    forecast = m.predict(future_all)

    forecast['ds'] = pd.to_datetime(forecast['ds'])
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds'])
    df_all = forecast.merge(df_prophet, how='left', on='ds')[['ds', 'y', 'yhat']]

    # --- Step 3: Creating running sums on actuals and predicted points ---
    df_all['actual_running_sub'] = df_all.sort_values('ds')['y'].cumsum()
    df_all['predicted_running_sub'] = df_all.sort_values('ds')['yhat'].cumsum()

    return df_all


def sub_metric_card(df, date_start_point):
    '''
    Return a plotly figure which will be used as a spark line in main file for the subscribers metric card

    df: Dataframe with predicted and actual values
    date_start_point: starting date for plotly figure
    '''
    df = df[df['ds'] >= date_start_point]

    fig = go.Figure()

    fig.add_trace(
            go.Scatter(
                x=df['ds'],
                y=df['actual_running_sub'],
                mode='lines',
                name='Actual',
                line=dict(dash='solid', color='blue', width=1)
            )
    )

    fig.add_trace(
            go.Scatter(
                x=df[df['actual_running_sub'].isna()]['ds'],
                y=df[df['actual_running_sub'].isna()]['predicted_running_sub'],
                mode='lines',
                name='Predicted',
                line=dict(dash='dot', color='red', width=1)
            )
    )

    first_x = df['ds'].sort_values().iloc[0]
    first_y = df['actual_running_sub'].sort_values().iloc[0]
    first_x_format = first_x.strftime('%b %Y')

    last_x = df['ds'].sort_values().iloc[-1] # Actual last value
    last_x_pos = df['ds'].sort_values().iloc[-7] # Using this for positioning when annotating as using last value will take value out of picture
    last_y = df['predicted_running_sub'].sort_values().iloc[-1]
    last_x_format = last_x.strftime('%b %Y')

    forecast_date = df[df['actual_running_sub'].isna()].sort_values('ds')['ds'].iloc[0]

    fig.add_vline(
        x=forecast_date,
        line=dict(color='lightgray', width=1.5, dash='dot'),
    )

    fig.add_annotation(
        x=first_x, 
        y=first_y, 
        text=f'{round(first_y)} ({first_x_format})',
        showarrow=False,
        xanchor='left',
        yanchor='top',
        font=dict(size=7)
    )

    fig.add_annotation(
        x=last_x_pos, 
        y=last_y, 
        text=f'{round(last_y)} ({last_x_format})',
        showarrow=False,
        xanchor='right',
        yanchor='bottom',
        font=dict(size=7)
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),  
        xaxis=dict(visible=False),          
        yaxis=dict(visible=False),      
        height=50,   
        showlegend=False                           
    )

    return fig


def get_forecast_figure():

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
    df = make_forecast(df, '2026-11-01')

    current_subs = int(df['y'].sum())

    deviation_6m = str(int(((df[df['ds'] == '2026-05-03']['predicted_running_sub'].iloc[0] / 1500) - 1) * 100)) + '%'
    deviation_12m = str(int(((df[df['ds'] == '2026-11-01']['predicted_running_sub'].iloc[0] / 2000) - 1) * 100)) + '%'

    total_deviation = deviation_6m + ' ' + deviation_12m
    
    fig = sub_metric_card(df, '2025-01-01')

    return current_subs, total_deviation, fig



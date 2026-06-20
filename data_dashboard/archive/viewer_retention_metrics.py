import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from prophet import Prophet
from datetime import datetime

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
    select * from pl.pl_dim_video_min_viewers
    """

df = run_sql_query(query)


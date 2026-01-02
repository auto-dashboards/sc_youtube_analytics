import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from prophet import Prophet
import plotly.graph_objects as go
from datetime import datetime
from subscribers_metric_card import get_forecast_figure
from metric_kpi_card import metric_forecast_stats, make_metric_card


st.set_page_config(layout="wide")

with st.container(width='stretch', height='stretch'):

    parent_sec1, parent_sec2, parent_sec3 = st.columns([1, 5, 2])

    with parent_sec1:
        with st.container(border=True, height='stretch', width='stretch'):
            first_col = st.columns(1)

            with first_col[0]:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Subscribers</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('net_subscribers')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('net_subscribers')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('net_subscribers')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

    with parent_sec2:
        with st.container(border=True, height='stretch', width='stretch'):
            second_col, third_col, fourth_col, fifth_col, sixth_col = st.columns(5)

            with second_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Comments</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('comments_count')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('comments_count')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('comments_count')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

            with third_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Views</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('views_count')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('views_count')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('views_count')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

            with fourth_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Shares</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('shares_count')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('shares_count')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('shares_count')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

            with fifth_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Likes</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('likes_count')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('likes_count')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('likes_count')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

            with sixth_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Dislikes</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('dislikes_count')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('dislikes_count')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('dislikes_count')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

    with parent_sec3:
        with st.container(border=True, height='stretch', width='stretch'):
            seventh_col, eighth_col = st.columns(2)

            with seventh_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Watch Time (min)</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats('estimated_watch_minutes')[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats('estimated_watch_minutes')[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats('estimated_watch_minutes')[2]
                    st.plotly_chart(fig, use_container_width='stretch')

            with eighth_col:
                with st.container(border=True, height='stretch', width='stretch'):
                    st.markdown(f"""
                    <div style="margin:0; padding:0; line-height:1;">
                        <div style="font-weight:600; font-size:15px; margin-bottom:2px;">Avg. View Duration (min)</div>
                        <div style="font-weight:700; font-size:23px; margin-bottom:3px;">{metric_forecast_stats(['estimated_watch_minutes', 'views_count'])[0]}</div>
                        <div style="font-weight:500; font-size:9px; margin-bottom:2px;">{metric_forecast_stats(['estimated_watch_minutes', 'views_count'])[1]}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    fig = metric_forecast_stats(['estimated_watch_minutes', 'views_count'])[2]
                    st.plotly_chart(fig, use_container_width='stretch')

 







       

       

    
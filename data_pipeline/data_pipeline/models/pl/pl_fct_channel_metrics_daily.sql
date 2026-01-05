{{
    config(
        materialized='view',
        schema='pl',
    )
}}

select distinct
    date as metric_date
    , record_source 
    , likes as likes_count
    , views as views_count
    , shares as shares_count
    , comments as comments_count
    , dislikes as dislikes_count
    , subscribersLost as subscribers_lost
    , subscribersGained as subscribers_gained
    , averageViewDuration as avg_view_duration_sec
    , averageViewPercentage as avg_view_percentage
    , estimatedMinutesWatched as estimated_watch_minutes

from {{ ref('fct_channel_metrics_daily') }}

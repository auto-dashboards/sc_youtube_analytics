{{
    config(
        materialized='view',
        schema='pl',
    )
}}

with src as (
    select 
        date as metric_date
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
        , row_number() over(partition by date order by load_ts desc)  as rn

    from {{ ref('fct_channel_metrics_daily') }} 

)

select 
    metric_date
    , likes_count
    , views_count
    , shares_count
    , comments_count
    , dislikes_count
    , subscribers_lost
    , subscribers_gained
    , avg_view_duration_sec
    , avg_view_percentage
    , estimated_watch_minutes

from src 
where rn = 1 

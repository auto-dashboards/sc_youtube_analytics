{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='day'
    )
}}

with src_stage_table as (
    select distinct 
        (json_rows ->> 'day')::date as day
        , (json_rows ->> 'likes')::int as likes
        , (json_rows ->> 'views')::int as views
        , (json_rows ->> 'shares')::int as shares
        , (json_rows ->> 'comments')::int as comments
        , (json_rows ->> 'dislikes')::int as dislikes
        , (json_rows ->> 'subscribersLost')::int as subscribersLost
        , (json_rows ->> 'subscribersGained')::int as subscribersGained
        , (json_rows ->> 'averageViewDuration')::int as averageViewDuration
        , (json_rows ->> 'averageViewPercentage')::float as averageView_percentage
        , (json_rows ->> 'estimatedMinutesWatched')::float as estimatedMinutesWatched
        , 'YOUTUBE' as platform
        , 'YOUTUBE_ANALYTICS_API' as record_source

    from {{ source('stage', 'sc_yt_day_data')}}
)

select
    day
    , platform
    , likes 
    , views
    , shares
    , comments
    , dislikes
    , subscribersLost
    , subscribersGained
    , averageViewDuration
    , averageView_percentage
    , estimatedMinutesWatched
    , current_timestamp as load_ts
    , record_source

from src_stage_table

{% if is_incremental() %}
where day not in (
    select day from {{ this }}
)
{% endif %}

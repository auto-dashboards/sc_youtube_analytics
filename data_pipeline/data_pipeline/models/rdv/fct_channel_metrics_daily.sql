{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='day'
    )
}}

with src_stage_table as (
    select distinct 
        date
        , (date_metrics ->> 'likes')::int as likes
        , (date_metrics ->> 'views')::int as views
        , (date_metrics ->> 'shares')::int as shares
        , (date_metrics ->> 'comments')::int as comments
        , (date_metrics ->> 'dislikes')::int as dislikes
        , (date_metrics ->> 'subscribersLost')::int as subscribersLost
        , (date_metrics ->> 'subscribersGained')::int as subscribersGained
        , (date_metrics ->> 'averageViewDuration')::int as averageViewDuration
        , (date_metrics ->> 'averageViewPercentage')::float as averageViewPercentage
        , (date_metrics ->> 'estimatedMinutesWatched')::float as estimatedMinutesWatched
        , load_ts
        , record_source

    from {{ source('stage', 'sc_yt_day_data')}}
)

select
    date
    , likes 
    , views
    , shares
    , comments
    , dislikes
    , subscribersLost
    , subscribersGained
    , averageViewDuration
    , averageViewPercentage
    , estimatedMinutesWatched
    , load_ts
    , record_source

from src_stage_table

{% if is_incremental() %}
where date not in (
    select date from {{ this }}
)
{% endif %}

{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='date',
        pre_hook="TRUNCATE TABLE {{ this }}"
        if var('truncate_reload', false)
        else none
    )
}}

with src_clean as (
    select * 
    from (
        select 
            *, 
            row_number() over(partition by date, date(load_ts) order by load_ts desc) as rn
        from {{ source('stage', 'sc_yt_day_data')}}
    ) t
    where rn = 1
)

, src as (
    select 
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

    from src_clean
)

, final_pull as (
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
        , md5(
            concat_ws('|', 
                likes::text,
                views::text,
                shares::text,
                comments::text,
                dislikes::text,
                subscribersLost::text,
                subscribersGained::text,
                averageViewDuration::text,
                averageViewPercentage::text,
                estimatedMinutesWatched::text
            )
        ) as hashdiff

    from src
)

, final_pull_dedupe as (
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
        , hashdiff

    from (
        select 
            *, 
            row_number() over (
                partition by date, hashdiff
                order by load_ts desc
            ) as rn
        from final_pull
    )
    where rn = 1
)

select 
    final_pull_dedupe.*

from final_pull_dedupe 

{% if is_incremental %}
where not exists (
    select 1 
    from {{ this }} as fct
    where fct.date = final_pull_dedupe.date 
        and fct.hashdiff = final_pull_dedupe.hashdiff
)
{% endif %}

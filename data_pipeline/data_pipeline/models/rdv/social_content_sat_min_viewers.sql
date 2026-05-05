{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='video_id',
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
            row_number() over(partition by video_id, date(load_ts) order by load_ts desc) as rn
        from {{ source('stage', 'sc_yt_video_data')}}
    ) t
    where rn = 1
)

, src as (
    select
        video_id
        , load_ts
        , key::int AS livestream_position
	    , (value ->> 'peakConcurrentViewers')::float as peak_concurrent_viewers
	    , (value ->> 'averageConcurrentViewers')::float as average_concurrent_viewers

    from src_clean

    cross join lateral jsonb_each(minute_metrics)
)

, final_pull as (
    select distinct
        video_id
        , load_ts
        , livestream_position
        , peak_concurrent_viewers
        , average_concurrent_viewers
        , md5(
            concat_ws('|', 
                livestream_position::text,
                peak_concurrent_viewers::text,
                average_concurrent_viewers::text
            )
        ) as hashdiff

    from src

)

, final_pull_dedupe as (
    select 
        video_id
        , livestream_position
        , peak_concurrent_viewers
        , average_concurrent_viewers
        , load_ts
        , hashdiff
    
    from (
        select
            *, 
            row_number() over(
                partition by video_id, hashdiff 
                order by load_ts desc
            ) as rn 
        from final_pull
    ) t
    where rn=1
)

select 
    final_pull_dedupe.*

from final_pull_dedupe 

{% if is_incremental %}
where not exists (
    select 1 
    from {{ this }} as sat
    where sat.video_id = final_pull_dedupe.video_id 
        and sat.hashdiff = final_pull_dedupe.hashdiff
)
{% endif %}

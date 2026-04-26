{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='video_id',
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
            coalesce(livestream_position, NULL) || '|' ||
            coalesce(peak_concurrent_viewers, NULL) || '|' ||
            coalesce(average_concurrent_viewers, NULL)
        ) as hashdiff

    from src
)

select 
    final_pull.*

from final_pull 

{% if is_incremental %}
where not exists (
    select 1 
    from {{ this }} as sat
    where sat.video_id = final_pull.video_id 
        and sat.hashdiff = final_pull.hashdiff
)
{% endif %}

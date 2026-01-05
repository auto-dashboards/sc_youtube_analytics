{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='video_id'
    )
}}

with social_content_hub as (
    select distinct
        video_id
        , record_source
    
    from {{ ref('social_content_hub')}}
)

, src as (
    select distinct
        sch.video_id
        , sch.record_source
        , stg.load_ts
        , key::int AS livestream_position
	    , (value ->> 'peakConcurrentViewers') as peak_concurrent_viewers
	    , (value ->> 'averageConcurrentViewers') as average_concurrent_viewers

    from {{ source('stage', 'sc_yt_video_data')}} as stg

    inner join social_content_hub as sch
        on stg.video_id = sch.video_id

    cross join lateral jsonb_each(stg.minute_metrics)
)

, final_pull as (
    select distinct
        src.video_id
        , src.record_source
        , src.load_ts
        , src.livestream_position
        , src.peak_concurrent_viewers
        , src.average_concurrent_viewers

    from src
)

select 
    final_pull.*

from final_pull 

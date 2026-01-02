{{
    config(
        materialized='view',
        schema='pl',
    )
}}

select distinct 
    video_id 
    , platform 
    , record_source
    , load_ts
    , livestreamposition as live_stream_positition 
    , peakconcurrentviewers as peak_concurrent_viewers
    , averageconcurrentviewers as average_concurrent_viewers

from {{ ref('social_content_sat_min_viewers') }}
order by video_id, live_stream_positition asc

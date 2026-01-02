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
        , platform
    
    from {{ ref('social_content_hub')}}
)

, src as (
    select distinct
        sch.video_id
        , sch.platform
        , stg.load_ts
        , (json_rows ->> 'livestreamPosition')::int as livestreamPosition
        , (json_rows ->> 'peakConcurrentViewers')::int as peakConcurrentViewers
        , (json_rows ->> 'averageConcurrentViewers')::int as averageConcurrentViewers

    from {{ source('stage', 'sc_yt_video_min_viewers')}} as stg

    inner join social_content_hub as sch
        on stg.json_rows ->> 'video_id' = sch.video_id
)

, final_pull as (
    select distinct
        src.video_id
        , src.platform
        , src.load_ts
        , src.livestreamPosition
        , src.peakConcurrentViewers
        , src.averageConcurrentViewers

    from src
)

select 
    final_pull.*
    , 'YOUTUBE_ANALYTICS_API' as record_source

from final_pull 

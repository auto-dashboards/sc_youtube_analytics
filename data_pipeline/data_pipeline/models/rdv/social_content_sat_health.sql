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
        , (json_rows ->> 'estimatedMinutesWatched')::int as estimatedMinutesWatched

    from {{ source('stage', 'sc_yt_video_health')}} as stg

    inner join social_content_hub as sch
        on stg.json_rows ->> 'video' = sch.video_id
)

, final_pull as (
    select distinct
        src.video_id
        , src.platform
        , src.load_ts
        , src.estimatedMinutesWatched
        , md5(
            coalesce(src.estimatedMinutesWatched::text, '')
        ) as hashdiff

    from src
)

select 
    final_pull.*
    , 'YOUTUBE_ANALYTICS_API' as record_source

from final_pull 

{% if is_incremental %}
where not exists (
    select 1 
    from {{ this }} as sat
    where sat.video_id = final_pull.video_id 
        and sat.hashdiff = final_pull.hashdiff
)
{% endif %}

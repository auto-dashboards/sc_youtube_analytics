{{
    config(
        materialized='view',
        schema='pl',
    )
}}

with social_content_sat_health as (
    select 
        video_id
        , platform 
        , estimatedminuteswatched
    
    from {{ ref('social_content_sat_health') }}
)

, src as (
    select distinct
        src.video_id
        , src.platform
        , src.video_title
        , src.video_description
        , src.video_published_at
        , src.video_duration_sec
        , src.video_topic
        , src.video_category
        , src.video_type
        , src.video_speaker
        , src.likesCount as video_likes
        , src.viewCount as video_views
        , src.commentCount as video_comments
        , health.estimatedminuteswatched as video_estimated_minutes_watched
        , row_number() over(partition by src.video_id, src.platform order by src.load_ts) as rn

    from {{ ref('social_content_sat_details') }} as src

    left join social_content_sat_health as health 
        on src.video_id = health.video_id

)

select distinct
    video_id
    , platform
    , video_title
    , video_description
    , video_published_at
    , video_duration_sec
    , video_topic
    , video_category
    , video_type
    , video_speaker
    , video_likes
    , video_views
    , video_comments
    , video_estimated_minutes_watched

from src
where rn = 1

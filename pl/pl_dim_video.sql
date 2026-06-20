{{
    config(
        materialized='view',
        schema='pl',
    )
}}

with src as (
    select distinct
        src.video_id
        , src.record_source
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
        , src.estimatedminuteswatched as video_estimated_minutes_watched
        , row_number() over(partition by src.video_id, src.record_source order by src.load_ts) as rn

    from {{ ref('social_content_sat_details') }} as src

)

select distinct
    video_id
    , record_source
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

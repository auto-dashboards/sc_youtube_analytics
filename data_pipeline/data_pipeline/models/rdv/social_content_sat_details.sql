{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='video_id',
        pre_hook=(
            "TRUNCATE TABLE {{ this }}"
            if var('truncate_reload', false)
            else ""
        )
    )
}}

-- Within each day, output the latest timestamp for every video
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
        , video_data -> 'snippet' ->> 'title' as video_title_raw
        , case
    	    when (video_data -> 'snippet' ->> 'title') ilike 'SESSION%' or (video_data -> 'snippet' ->> 'title') ILIKE 'Series%' 
    			then trim(split_part(video_data -> 'snippet' ->> 'title', '|', 2))
    	    when (video_data -> 'snippet' ->> 'title') ilike '[Luton%' 
			    then trim(split_part(video_data -> 'snippet' ->> 'title', '-', 1))    	
		    else trim(split_part(video_data -> 'snippet' ->> 'title', '|', 1))
        end as video_title
        , nullif(video_data -> 'snippet' ->> 'description', NULL) as video_description
        , (video_data -> 'snippet' ->> 'publishedAt')::timestamp as video_published_at
        , EXTRACT(EPOCH from (video_data -> 'contentDetails' ->> 'duration')::interval) as video_duration_sec
        , video_data -> 'topicDetails' ->> 'topicCategories' as video_topic
        , video_data -> 'snippet' ->> 'categoryId' as video_category
        , (video_data -> 'statistics' ->> 'likeCount')::int as likesCount
        , (video_data -> 'statistics' ->> 'viewCount')::int as viewCount
        , (video_data -> 'statistics' ->> 'commentCount')::int as commentCount
        , estimatedminuteswatched 
        , load_ts

    from src_clean
)

, final_pull as (
    select 
        src.video_id
        , src.video_title_raw
        , src.video_title
        , src.video_description
        , src.video_published_at
        , src.video_duration_sec
        , src.video_topic
        , src.video_category
        , src.likesCount
        , src.viewCount
        , src.commentCount
        , src.estimatedminuteswatched
        , src.load_ts
        , md5(
            concat_ws('|', 
                src.video_title::text,
                src.video_description::text,
                src.video_published_at::text,
                src.video_duration_sec::text,
                src.video_topic::text,
                src.video_category::text,
                src.likesCount::text,
                src.viewCount::text,
                src.commentCount::text,
                src.estimatedminuteswatched::text
            ) 
        ) as hashdiff

    from src

)
-- For each video, dedupe duplicate hash diffs
, final_pull_dedupe as (
    select 
        video_id
        , video_title_raw
        , video_title
        , video_description
        , video_published_at
        , video_duration_sec
        , video_topic
        , video_category
        , likesCount
        , viewCount
        , commentCount
        , estimatedminuteswatched
        , load_ts
        , hashdiff

    from (
        select 
            *, 
            row_number() over (
                partition by video_id, hashdiff
                order by load_ts desc
            ) as rn
        from final_pull
    ) t
    where rn = 1
)

select 
    final_pull_dedupe.*

from final_pull_dedupe 

{% if is_incremental() and not var('truncate_reload', false) %}
where not exists (
    select 1 
    from {{ this }} as sat
    where sat.video_id = final_pull_dedupe.video_id 
        and sat.hashdiff = final_pull_dedupe.hashdiff
)
{% endif %}

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
        , json_rows -> 'snippet' ->> 'title' as video_title_raw
        , case
    	    when (json_rows -> 'snippet' ->> 'title') ilike 'SESSION%' or (json_rows -> 'snippet' ->> 'title') ILIKE 'Series%' 
    			then trim(split_part(json_rows -> 'snippet' ->> 'title', '|', 2))
    	    when (json_rows -> 'snippet' ->> 'title') ilike '[Luton%' 
			    then trim(split_part(json_rows -> 'snippet' ->> 'title', '-', 1))    	
		    else trim(split_part(json_rows -> 'snippet' ->> 'title', '|', 1))
        end as video_title
        , nullif(json_rows -> 'snippet' ->> 'description', NULL) as video_description
        , json_rows -> 'snippet' ->> 'publishedAt' as video_published_at
        , EXTRACT(EPOCH from (json_rows -> 'contentDetails' ->> 'duration')::interval) as video_duration_sec
        , json_rows -> 'topicDetails' ->> 'topicCategories' as video_topic
        , json_rows -> 'snippet' ->> 'categoryId' as video_category
        , (json_rows -> 'statistics' ->> 'likeCount')::int as likesCount
        , (json_rows -> 'statistics' ->> 'viewCount')::int as viewCount
        , (json_rows -> 'statistics' ->> 'commentCount')::int as commentCount

    from {{ source('stage', 'sc_yt_video_data')}} as stg

    inner join social_content_hub as sch
        on stg.json_rows ->> 'id' = sch.video_id
)

, src_video_type as (
    select distinct
        src.video_id
        , case
            when (src.video_title_raw like '[Luton 2023]%') or (src.video_title_raw like '[Luton 2024]%') then 'Luton Livestream'
            when (src.video_duration_sec)::int <= 60 then 'Shorts'
            when ((src.video_duration_sec)::int > 60) and ((src.video_duration_sec)::int <= 1200) then 'Other'
            when (src.video_duration_sec)::int > 1200 then 'Livestream'
        else null
        end as video_type    

    from src 
)

, src_video_speaker as (
    select distinct
	    video_id
        , coalesce(
            case 
                when video_type = 'Livestream' then pipe_parts
                when video_type = 'Luton Livestream' then dash_parts
            else null
            end, 
            pipe_parts, 
            dash_parts
        ) as video_speaker_raw
		
    from (
        select distinct
            src.video_id
            , src_video_type.video_type
            , trim((string_to_array(video_title, '-'))[2]) as dash_parts
            , case 
                when (video_title_raw like 'SESSION%') or (video_title_raw like 'Series%') then trim((string_to_array(video_title_raw, '|'))[4])
                when (video_title_raw like '[Luton 2023]%') or (video_title_raw like '[Luton 2024]%') then trim((string_to_array(video_title_raw, '|'))[2]) 
                else coalesce(trim((string_to_array(video_title_raw, '|'))[3]), trim((string_to_array(video_title_raw, '|'))[2]))
            end as pipe_parts
        from src

        left join src_video_type
            on src.video_id = src_video_type.video_id
    ) as a 
)

, src_video_speaker_correction as (
    select distinct
        src.video_id
        , nullif(crt.canonical_name, NULL) as video_speaker
    
    from src_video_speaker as src
    left join {{ ref('speaker_name_correction')}} as crt
        on src.video_speaker_raw = crt.original_name
)

, final_pull as (
    select distinct
        src.video_id
        , src.platform
        , src.video_title_raw
        , src.video_title
        , src.video_description
        , src.video_published_at
        , src.video_duration_sec
        , src.video_topic
        , src.video_category
        , src_video_type.video_type
        , src_video_speaker_correction.video_speaker
        , src.likesCount
        , src.viewCount
        , src.commentCount
        , src.load_ts
        , md5(
            coalesce(src.platform, '') || '|' ||
            coalesce(src.video_title, '') || '|' ||
            coalesce(src.video_description, '') || '|' ||
            coalesce(src.video_published_at, '') || '|' ||
            coalesce(src.video_duration_sec::text, '') || '|' ||
            coalesce(src.video_topic, '') || '|' ||
            coalesce(src.video_category, '') || '|' ||
            coalesce(src_video_type.video_type, '') || '|' ||
            coalesce(src_video_speaker_correction.video_speaker, '') || '|' ||
            coalesce(src.likesCount, NULL) || '|' ||
            coalesce(src.viewCount, NULL) || '|' ||
            coalesce(src.commentCount, NULL)
        ) as hashdiff

    from src

    left join src_video_type
        on src.video_id = src_video_type.video_id

    left join src_video_speaker_correction
        on src.video_id = src_video_speaker_correction.video_id
)

select 
    final_pull.*
    , 'YOUTUBE_DATA_API' as record_source

from final_pull 

{% if is_incremental %}
where not exists (
    select 1 
    from {{ this }} as sat
    where sat.video_id = final_pull.video_id 
        and sat.hashdiff = final_pull.hashdiff
)
{% endif %}

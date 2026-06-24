{{
    config(
        materialized='incremental',
        schema='bdv',
        unique_key=['video_id', 'minute_boundaries_lb']
    )
}}

with src_stage_table as (
    select 
        video_id
        , start_time
        , end_time
        , text

    from {{ source('stage', 'sc_yt_video_transcript')}}
    where video_id is not null

    {% if is_incremental() %}
    and video_id not in (
        select distinct video_id
        from {{ this }}
    )
    {% endif %}
)

-- Need to identify whether time ranges span multiple 60 second brackets. If they do, need to split them. 
-- If the start time and end time belong in different 60 second brackets, then it spans multiple 60s bracket
-- E.g. start_time - 58.6 and end_time - 63.8 has the following text 'So the gathering for those who don't know or need to be reminded is divided into three'
-- This is within the brackets 0 - 60s and 60s - 120s. So we duplicate the row. In row 1 - start_time - 58.6 and end_time - 60. Row 2 - start_time - 60 and end_time - 63.8
-- Same text
, multi_buckets_span as (
    select 
        video_id
        , start_time
        , end_time
        , text
        , floor(start_time / 60) * 60 as start_time_lb_60
        , floor(end_time / 60) * 60 as end_time_lb_60
        , floor(start_time / 60) * 60 != floor(end_time / 60) * 60 as span_multi_buckets

    from src_stage_table
)

, multi_span_one as (
    select 
        video_id
        , start_time
        , end_time_lb_60 as end_time
        , text 

    from multi_buckets_span
    where span_multi_buckets = True
)

, multi_span_two as (
    select 
        video_id
        , end_time_lb_60 as start_time
        , end_time
        , text 

    from multi_buckets_span
    where span_multi_buckets = True
)

, single_span_all as (
    select 
        video_id
        , start_time
        , end_time
        , text 

    from multi_buckets_span
    where span_multi_buckets = False
)

, video_timestamp_update as (
    select distinct 
        video_id
        , start_time
        , end_time
        , text 

    from (
        select video_id, start_time, end_time, text from multi_span_one
        union all 
        select video_id, start_time, end_time, text from multi_span_two
        union all
        select video_id, start_time, end_time, text from single_span_all
    )

    order by video_id, start_time
)

, video_timestamp_update_duration as (
    select 
        video_id
        , start_time
        , end_time
        , end_time - start_time as duration_secs
        , case 
            when text != '' then end_time - start_time
            else 0 
        end as duration_secs_valid
        , text

    from video_timestamp_update
)

, video_60_boundaries as (
    select 
        video_id
        , start_time
        , end_time
        , floor(start_time / 60) * 60 as start_time_lb_60
        , (floor(start_time / 60) * 60) + 60 as start_time_ub_60
        , duration_secs_valid
        , text

    from video_timestamp_update_duration
)

, video_60_boundaries_group as (
    select 
        video_id
        , start_time_lb_60 as minute_boundaries_lb
        , start_time_ub_60 as minute_boundaries_ub
        , sum(duration_secs_valid) as duration_secs_valid
        , string_agg(text, '') as text

    from video_60_boundaries
    group by video_id, minute_boundaries_lb, minute_boundaries_ub
)

select
    video_id
    , minute_boundaries_lb
    , minute_boundaries_ub
    , duration_secs_valid
    , text

from video_60_boundaries_group

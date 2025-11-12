{{
    config(
        materialized='incremental',
        schema='rdv',
        unique_key='video_id'
    )
}}

with src_stage_table as (
    select distinct 
        nullif(json_rows ->> 'id', '~~') as video_id
        , 'YOUTUBE' as platform
        , current_timestamp as load_ts

    from {{ source('stage', 'sc_yt_video_data')}}
    where nullif(json_rows ->> 'id', '~~') is not null
)

select
    video_id
    , platform
    , load_ts

from src_stage_table

{% if is_incremental() %}
    where not exists (
        select 1 
        from {{ this }} as hub 
        where hub.video_id = src_stage_table.video_id
    )
{% endif %}

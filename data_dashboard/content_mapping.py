import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import math
import os
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def run_sql_query(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(sql.SQL(query).as_string(conn), conn)
        return df 
    finally: 
        conn.close()


def video_transcript_convert(id):
    '''
    Convert a raw transcript file into 60-second livestream chunks and merge with minute-level viewership metrics 

    Processing steps: 
    1. Load transcript segments from the video's transcript file

    2. Parse transcript timestamps into start time and end time columns 

    3. Identify gaps between consecutive transcript segments and create empty rows to represent periods where no transcript
    text exists (typically due to audio dropouts, livestream issues, or transcript extraction failures)

    4. Split transcript segments that span multiple 60-second buckets.
    Example: 354.12 -> 368.08
    Becomes: 354.12 -> 360.00 and 360.00 -> 368.08
    This ensures each transcript segment belongs entirely to a single 60-second bucket and prevents text from being incorrectly 
    assigned across minute boundaries 

    5. Calculate transcript coverage statistics: 
    - duration_secs: Total duration of the transcript segment 
    - duration_secs_valid: Duration contributed by non-empty transcript text only 
    - duration_text_coverage_pct: Percentage of the 60 second bucket covered by valid transcript text. 

    6. Aggregate transcript text into 60-second buckets:
    - Concant all transcript text belonging to the same minute 
    - Sum valid transcript duration within the minute

    7. Merge aggregated transcript buckets with min-level livestream numbers (avg viewers, peak viewers etc)

    Returns:
    pd.DataFrame

    One row per 60-second livestream interval containing: 

    - video_id

    - livestream position - lower boundary of the min bucket 

    - peak_concurrent_viewers - Peak viewers observed during the min 

    - average_concurrent_viewers - Average viewers observed during the min 

    - text - All transcript text occuring within the min bucket 

    - duration_secs_valid - Number of seconds of valid transcript text avaialble in the min bucket

    - duration_text_coverage_pct - Fraction of the min covered by transcript text (duration_secs_valid / 60)

    Notes: 
    - Missing transcript periods are preserved as empty text rather than discarded. 
    This allows checks between content and audio/transcript dropouts and also alignment with min-level viewership metrics
    '''

    video_transcript_path = f'data_dashboard/video_transcripts/{id}.txt'

    df_transcript = pd.read_csv(
        video_transcript_path,
        sep=']',
        names=['time', 'text']
    )

    df_transcript['video_id'] = id

    df_transcript['time'] = df_transcript['time'].str.replace('[', '')

    df_transcript[['start_time', 'end_time']] = (
        df_transcript['time']
        .str.split('->', expand=True)
        .astype(float)
    )

    empty_rows = []

    for i in range(len(df_transcript) - 1): # this current logic won't work for the last row, as there's no next_start for it, so we need '-1'

        current_end = df_transcript.iloc[i]['end_time']
        next_start = df_transcript.iloc[i+1]['start_time']

        if current_end != next_start:
            empty_rows.append({
                'start_time': current_end,
                'end_time': next_start,
                'text': '',
                'video_id': id
            })

    df_empty_rows = pd.DataFrame(empty_rows)

    df_transcript = pd.concat([df_transcript, df_empty_rows]).sort_values('start_time', ascending=True)

    '''
    354.12 -> 368.08 falls in 2 buckets - 300 - 360 and 360 - 420
    Need to split it like this - 354.12-360 goes into the bucket 300-360 and 360-368.08 goes into the bucket 360-420

    First need to identify whether a timestamp crosses multiple buckets. If the lower boundary of the 'multiple of 60' bucket is the same number for start time 
    and end time then it doesn't.
    '''
    df_transcript['start_time_lb_60'] = (df_transcript['start_time'] // 60 * 60).astype(int)
    df_transcript['end_time_lb_60'] = (df_transcript['end_time'] // 60 * 60).astype(int)
    df_transcript['multiple_bucket_span'] = df_transcript['start_time_lb_60'] != df_transcript['end_time_lb_60']

    df_transcript_multi_span_one = df_transcript[df_transcript['multiple_bucket_span'] == True]
    df_transcript_multi_span_one['end_time'] = df_transcript_multi_span_one['end_time_lb_60']

    df_transcript_multi_span_two = df_transcript[df_transcript['multiple_bucket_span'] == True]
    df_transcript_multi_span_two['start_time'] = df_transcript_multi_span_two['end_time_lb_60']

    df_transcript_single_span = df_transcript[df_transcript['multiple_bucket_span'] == False]

    df_transcript_ts_update = (
        pd.concat([df_transcript_multi_span_one, df_transcript_multi_span_two, df_transcript_single_span])
        .sort_values('start_time', ascending=True)
    )[['time', 'text', 'video_id', 'start_time', 'end_time']]

    df_transcript_ts_update['duration_secs'] = df_transcript_ts_update['end_time'] - df_transcript_ts_update['start_time']

    df_transcript_ts_update['duration_secs_valid'] = np.where(
        df_transcript_ts_update['text'] != '',
        df_transcript_ts_update['duration_secs'],
        0
    )

    df_transcript_ts_update['start_time_lb_60'] = ((
        df_transcript_ts_update['start_time'] // 60 * 60)
        .astype(int) 
    ) # check which lower boundary of 60 seconds this belongs in

    df_transcript_ts_update['start_time_ub_60'] = (((
        df_transcript_ts_update['start_time'] // 60 * 60) + 60)
        .astype(int)
    ) # check which upper boundary of 60 seconds this belongs in

    df_transcript_ts_update['minute_boundaries'] = (
        df_transcript_ts_update['start_time_lb_60'].astype(str)
        + '-'
        + df_transcript_ts_update['start_time_ub_60'].astype(str)
    )

    # Example of 2 60s boundaries
    # df_transcript_ts_update[df_transcript_ts_update['time'] == '117.00 -> 125.00']

    df_transcript_grouped = (
        df_transcript_ts_update
        .groupby(
            ['video_id', 'start_time_lb_60', 'start_time_ub_60', 'minute_boundaries'],
            as_index=False
        )
        .agg(
            text=('text', ''.join),
            duration_secs_valid=('duration_secs_valid', 'sum')
        )
    )

    df_transcript_grouped['duration_text_coverage_pct'] = df_transcript_grouped['duration_secs_valid'] / 60

    # df_comb = (
    #     df[df['video_id'] == id]
    #     .merge(df_transcript_grouped, left_on=['video_id', 'livestream_position'], right_on=['video_id', 'start_time_lb_60'], how='left')
    # )[['video_id', 'livestream_position', 'peak_concurrent_viewers', 'average_concurrent_viewers', 'text', 'duration_secs_valid', 'duration_text_coverage_pct']]

    df_transcript_grouped['text'] = df_transcript_grouped['text'].fillna('')

    return df_transcript_grouped


def video_transcript_semantic_shift(df, context_window):
    '''
    Calculate semnatic topic shifts throughout a livestream 

    Each minute of transcript text is converted into an embedding vector
    using a sentence transfomer model. The embedding for the current minute 
    is compared against the average embedding of the previous N mins using cosine similarity. 

    Semantic shift is calculated as: 
        semantic_shift = 1 - cosine_similarity 

    A low score indicates the speaker is continuing the current topic, while a high score
    indicates a move to a different topic.

    The resulting semantic shift scores can be analysed alongside viewer metrics to identify 
    whether a topic changes are associated with increases or decreases in audience retention.

    Params:
    df: pd.DataFrame
        Cleaned transcript file segmented by minute buckets with corresponding text and viewer metrics. Converted into a dataframe

    context_window: int
        Compare the current minute to the average of the previous N minutes. 
        context_window = 1 compares the current minute to the immediately preceding minute. This measures short term semantic movement.
        context_window = 5 compares the current minute to the average of previous 5 minutes. This measures movement to the recent discussion

    Returns: pd.DataFrame
        Livestream min by min metrics merged with transcript text per min and semantic shift scores
    
    '''

    df = df.copy()

    df['text'] = (
        np.where(df['duration_text_coverage_pct'] < 0.50,
                 '',
                 df['text']
        )
    )

    model = SentenceTransformer('all-MiniLM-L6-v2')

    embeddings = model.encode(
        df['text'].tolist(),
        convert_to_numpy=True
    )

    # Change the embedding vectors to 0, for rows where text is ''
    mask = df['text'].eq('')
    embeddings[mask] = np.nan

    # Compare embedding vectors of current vs average of previous 5
    # Need the first 5 to begin with 0 as there's no previous average to compare with
    topic_shift = [0] * context_window
    for i in range(context_window, len(embeddings)):
        # print(i)
        current = embeddings[i]
        window = embeddings[i-context_window:i]

        valid_embeddings = [
            e for e in window
            if not np.isnan(e).all()
        ]

        if (context_window == 5) and (len(valid_embeddings) >= 3):
            avg = np.mean(
                valid_embeddings,
                axis=0
            )

        elif (context_window == 4) and (len(valid_embeddings) >= 2):
            avg = np.mean(
                valid_embeddings,
                axis=0
            )

        elif (context_window in [3, 2, 1]) and (len(valid_embeddings) >= 1):
            avg = np.mean(
                valid_embeddings,
                axis=0
            )

        else:
            avg = np.nan

        if (np.isnan(current).all() == True) or (np.isnan(avg).all() == True):
            topic_shift.append(None)
        else:
            similarity = cosine_similarity(
                [current],
                [avg]
            )[0][0]

            topic_shift.append(
                1 - similarity
            )

    df[f'semantic_shift_{context_window}m'] = topic_shift

    return df

query = """
    with min_viewers as (
        select
            video_id
            , livestream_position 
            , peak_concurrent_viewers 
            , average_concurrent_viewers 
            , row_number() over(partition by video_id, livestream_position order by load_ts desc) as rn
            
        from rdv.social_content_sat_min_viewers 
    )

    , video_details as (
        select distinct 
            video_id
            , video_duration_sec
        
        from rdv.social_content_sat_details 
    )

    , base as (
        select 
            mv.video_id
            , mv.livestream_position 
            , mv.peak_concurrent_viewers 
            , mv.average_concurrent_viewers 
        
        from min_viewers as mv 
        
        left join video_details as vd 
            on mv.video_id = vd.video_id
        
        where 1=1 
            and mv.rn = 1
            and video_duration_sec < 5400
    )

    select *
    from base
"""

df_orig = run_sql_query(query)
ids = ['7JgONHx-7TQ', 'Exf9BUhrUPU', 'iBxZe4V9bCw', 'oW9bkXo8zjc', 'pCHJLbidaZ4']

df_all = []
for id in ids: 

    df_transcript_clean = video_transcript_convert(df_orig, id)
    df_semantic_context_analysis = video_transcript_semantic_shift(df_transcript_clean, 5)
    df_semantic_local_analysis = video_transcript_semantic_shift(df_transcript_clean, 1)

    df_semantic_shift_all = (
        df_semantic_context_analysis
        .merge(df_semantic_local_analysis[['video_id', 'livestream_position', 'semantic_shift_1m']]
            , on=['video_id', 'livestream_position']
            , how='left')
    )

## --------- NEED TO EMBED INTO A FUNCTION

    df_semantic_shift_all['viewer_shift'] = (
        df_semantic_shift_all
        .groupby('video_id')['average_concurrent_viewers']
        .diff()
    )

    df_semantic_shift_all['viewer_shift_segment'] = np.select(
        [(df_semantic_shift_all['viewer_shift'] >= 2),
        (df_semantic_shift_all['viewer_shift'].between(-1, 1)), 
        (df_semantic_shift_all['viewer_shift'] < -1), 
        ],

        ['Gain', 
        'Retain', 
        'Dip',
        ],

        default='NULL'
    ) 

    df_all.append(df_semantic_shift_all)

df_all = pd.concat(df_all, ignore_index=True)

avg = df_all[df_all['semantic_shift_5m'] != 0]['semantic_shift_5m'].mean()
std = df_all[df_all['semantic_shift_5m'] != 0]['semantic_shift_5m'].std()

# 1 SD boundaries 
lower_1sd = avg - std 
upper_1sd = avg + std 

# 2 SD boundaries 
lower_2sd = avg - 2 * std
upper_2sd = avg + 2 * std

conditions = [
    df_all['semantic_shift_5m'] < lower_2sd, 
    (df_all['semantic_shift_5m'] >= lower_2sd) & (df_all['semantic_shift_5m'] < lower_1sd), 
    (df_all['semantic_shift_5m'] >= lower_1sd) & (df_all['semantic_shift_5m'] <= upper_1sd), 
    (df_all['semantic_shift_5m'] > upper_1sd) & (df_all['semantic_shift_5m'] <= upper_2sd), 
    df_all['semantic_shift_5m'] > upper_2sd, 
]

bands = [
    'Very Low Change',
    'Low Change', 
    'Moderate Change', 
    'High Change',
    'Very High Change'
]

df_all['semantic_shift_5m_seg'] = np.select(
    conditions,
    bands,
    default='NULL'
) 

df_all.to_clipboard()






######## IMAM HELP

id = 'fget.io_1781683202574'

video_transcript_path = f'data_dashboard/video_transcripts/{id}.txt'

df_transcript = pd.read_csv(
    video_transcript_path,
    sep=']',
    names=['time', 'text']
)

df_transcript['video_id'] = id

df_transcript['time'] = df_transcript['time'].str.replace('[', '')

df_transcript[['start_time', 'end_time']] = (
    df_transcript['time']
    .str.split('->', expand=True)
    .astype(float)
)

df_imam = ''.join(df_transcript['text'].astype(str))

pd.Series([df_imam]).to_clipboard()


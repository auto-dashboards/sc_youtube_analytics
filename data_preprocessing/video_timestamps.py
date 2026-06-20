from faster_whisper import WhisperModel
import time
from yt_dlp import YoutubeDL
import pandas as pd
import numpy as np
import os

# os.environ["PATH"] += os.pathsep + r"C:\tools\ffmpeg\bin"

def download_video_audio(url):
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": r"data_preprocessing\video_audio\%(id)s.%(ext)s",
        "postprocessors": [{
            "key": "FFmpegMetadata",
        }],
        "noplaylist": True,
        "extractor_args": {
            "youtube": {"player_client": ["android"]}
        },
        "ffmpeg_location": r"C:\tools\ffmpeg\bin\ffmpeg.exe"
    }

    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


def download_video_transcript(video_id):

    audio_path = f'data_preprocessing/video_audio/{video_id}.mp4'
    transcript_path = f'data_preprocessing/video_transcripts/{video_id}.txt'

    model = WhisperModel(
        'medium', 
        device='cpu',
        compute_type='int8',
        cpu_threads=8
    ) 

    start_time = time.time()

    segments, info = model.transcribe(
        audio_path,
        language='en',
        beam_size=5,
        vad_filter=False, # this filter if true, will remove parts that it doesn't think are speech
        condition_on_previous_text=False, # stops Whisper from getting stuck on 'English' words, as we have arabic too
        task='transcribe'
    )

    print(
        f'Detected languages: '
        f'{info.language} '
        f'({info.language_probability:.2f})'
    )

    end_time = time.time()
    print(f'Audio transcribed in {end_time - start_time:.2f} seconds')

    start_time = time.time()
    flush_every = 5
    with open(transcript_path, 'w', encoding='utf-8') as f:
        for i, segment in enumerate(segments, start=1):
            f.write(f'[{segment.start:.2f} -> {segment.end:.2f}] {segment.text}\n')
            
            if i % flush_every == 0:
                f.flush()

    end_time = time.time()
    print(f'Audio transcription written in {end_time - start_time:.2f} seconds')


def video_transcript_clean(video_id):
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

    video_transcript_path = f'data_preprocessing/video_transcripts/{video_id}.txt'

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

    df_transcript = pd.concat([df_transcript, df_empty_rows]).sort_values('start_time', ascending=True)[['video_id', 'start_time', 'end_time', 'text']]

    # '''
    # 354.12 -> 368.08 falls in 2 buckets - 300 - 360 and 360 - 420
    # Need to split it like this - 354.12-360 goes into the bucket 300-360 and 360-368.08 goes into the bucket 360-420

    # First need to identify whether a timestamp crosses multiple buckets. If the lower boundary of the 'multiple of 60' bucket is the same number for start time 
    # and end time then it doesn't.
    # '''
    # df_transcript['start_time_lb_60'] = (df_transcript['start_time'] // 60 * 60).astype(int)
    # df_transcript['end_time_lb_60'] = (df_transcript['end_time'] // 60 * 60).astype(int)
    # df_transcript['multiple_bucket_span'] = df_transcript['start_time_lb_60'] != df_transcript['end_time_lb_60']

    # df_transcript_multi_span_one = df_transcript[df_transcript['multiple_bucket_span'] == True]
    # df_transcript_multi_span_one['end_time'] = df_transcript_multi_span_one['end_time_lb_60']

    # df_transcript_multi_span_two = df_transcript[df_transcript['multiple_bucket_span'] == True]
    # df_transcript_multi_span_two['start_time'] = df_transcript_multi_span_two['end_time_lb_60']

    # df_transcript_single_span = df_transcript[df_transcript['multiple_bucket_span'] == False]

    # df_transcript_ts_update = (
    #     pd.concat([df_transcript_multi_span_one, df_transcript_multi_span_two, df_transcript_single_span])
    #     .sort_values('start_time', ascending=True)
    # )[['time', 'text', 'video_id', 'start_time', 'end_time']]

    # df_transcript_ts_update['duration_secs'] = df_transcript_ts_update['end_time'] - df_transcript_ts_update['start_time']

    # # df_transcript_ts_update['duration_secs_valid'] = np.where(
    # #     df_transcript_ts_update['text'] != '',
    # #     df_transcript_ts_update['duration_secs'],
    # #     0
    # # )

    # df_transcript_ts_update['start_time_lb_60'] = ((
    #     df_transcript_ts_update['start_time'] // 60 * 60)
    #     .astype(int) 
    # ) # check which lower boundary of 60 seconds this belongs in

    # df_transcript_ts_update['start_time_ub_60'] = (((
    #     df_transcript_ts_update['start_time'] // 60 * 60) + 60)
    #     .astype(int)
    # ) # check which upper boundary of 60 seconds this belongs in

    # df_transcript_ts_update['minute_boundaries'] = (
    #     df_transcript_ts_update['start_time_lb_60'].astype(str)
    #     + '-'
    #     + df_transcript_ts_update['start_time_ub_60'].astype(str)
    # )

    # # Example of 2 60s boundaries
    # # df_transcript_ts_update[df_transcript_ts_update['time'] == '117.00 -> 125.00']

    # df_transcript_grouped = (
    #     df_transcript_ts_update
    #     .groupby(
    #         ['video_id', 'start_time_lb_60', 'start_time_ub_60'],
    #         as_index=False
    #     )
    #     .agg(
    #         text=('text', ''.join),
    #         # duration_secs_valid=('duration_secs_valid', 'sum')
    #     )
    # )

    # # df_transcript_grouped['duration_text_coverage_pct'] = df_transcript_grouped['duration_secs_valid'] / 60
    # df_transcript_grouped['text'] = df_transcript_grouped['text'].fillna('')

    return df_transcript


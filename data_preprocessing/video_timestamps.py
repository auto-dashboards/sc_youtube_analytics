import time
from yt_dlp import YoutubeDL
from faster_whisper import WhisperModel
import pandas as pd
import numpy as np
import os


def download_video_audio(url):
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": "data_preprocessing/video_audio/%(id)s.%(ext)s",
        "postprocessors": [{"key": "FFmpegMetadata"}],
        "noplaylist": True,
        "extractor_args": {
            "youtube": {"player_client": ["android"]}
        }
        # "ffmpeg_location": r"C:\tools\ffmpeg\bin\ffmpeg.exe"
    }

    with YoutubeDL(ydl_opts) as ydl:
        print(f'Downloading video: {url}')
        ydl.download([url])


def load_whisper_model():

    return WhisperModel(
        'medium', 
        device='cpu',
        compute_type='int8',
        cpu_threads=8
    ) 


def download_video_transcript(video_id, model):

    audio_path = f'data_preprocessing/video_audio/{video_id}.mp4'
    transcript_path = f'data_preprocessing/video_transcripts/{video_id}.txt'

    start_time = time.time()

    segments, info = model.transcribe(
        audio_path,
        language='en',
        beam_size=5,
        vad_filter=False, # this filter if true, will remove parts that it doesn't think are speech
        condition_on_previous_text=False, # stops Whisper from getting stuck on 'English' words, as we have arabic too
        task='transcribe'
    )

    print(f'Detected languages: {info.language} ({info.language_probability:.2f})')
    print(f'Audio transcribed in {time.time() - start_time:.2f} seconds')

    os.makedirs(os.path.dirname(transcript_path), exist_ok=True) # check if folder exists, if it doesn't then create it

    start_time = time.time()
    with open(transcript_path, 'w', encoding='utf-8') as f:
        for segment in segments:
            f.write(f'[{segment.start:.2f} -> {segment.end:.2f}] {segment.text}\n')

    print(f'Audio transcription written in {time.time() - start_time:.2f} seconds')




def video_transcript_clean(video_id):

    """
    Clean and standardise a YouTube transcript file.

    Steps:
    1. Load the transcript text file for the specified video.
    2. Split transcript timestamps into start_time and end_time columns.
    3. Add the video_id to each transcript row.
    4. Identify gaps between transcript segments.
    5. Create empty transcript rows for any missing time periods.
    6. Combine original and generated rows.
    7. Sort by start_time and return a clean transcript DataFrame.

    Parameters
    ----------
    video_id : str
        YouTube video ID.

    Returns
    -------
    pandas.DataFrame
        Transcript data containing:
        - video_id
        - start_time
        - end_time
        - text

    Notes
    -----
    Empty rows are inserted whenever there is a gap between the
    end_time of one transcript segment and the start_time of the
    next segment. This ensures continuous timestamp coverage.
    """

    video_transcript_path = f'data_preprocessing/video_transcripts/{video_id}.txt'

    df_transcript = pd.read_csv(
        video_transcript_path,
        sep=']',
        names=['time', 'text']
    )

    df_transcript['video_id'] = video_id

    df_transcript['time'] = df_transcript['time'].str.replace('[', '', regex=False)

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
                'video_id': video_id
            })

    df_empty_rows = pd.DataFrame(empty_rows)

    df_transcript = pd.concat([df_transcript, df_empty_rows]).sort_values('start_time', ascending=True)[['video_id', 'start_time', 'end_time', 'text']]

    return df_transcript



# df = video_transcript_clean('tt1_5_5h4yqK7uiIE')

# combined = '\n'.join(df['text'].astype(str))
# pd.Series([combined]).to_clipboard(index=False, header=False)

  
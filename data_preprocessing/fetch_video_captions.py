from youtube_transcript_api import YouTubeTranscriptApi
import pandas as pd

transcript = YouTubeTranscriptApi().fetch('p1BGhYMAjeM')

for item in transcript:
    print(item.text)


def video_captions_clean(video_id):

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

    video_captions = YouTubeTranscriptApi().fetch(video_id)

    captions_text = [video_captions[i].text for i in range(len(video_captions))]
    captions_start = [video_captions[i].start for i in range(len(video_captions))]
    captions_duration = [video_captions[i].duration for i in range(len(video_captions))]
 
    df_captions = pd.DataFrame({
        'video_id': video_id,
        'video_caption': captions_text,
        'video_captions_start_time': captions_start,
        'video_captions_duration': captions_duration
    })

    df_captions['video_captions_end_time'] = df_captions['video_captions_start_time'] + df_captions['video_captions_duration']


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
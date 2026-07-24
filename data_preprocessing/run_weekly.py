import os 
import argparse
from dotenv import load_dotenv
import helper_functions
import boto3
from pathlib import Path
import pandas as pd
import video_timestamps

def main(mode):

    # === Load environment variables from .env file ===
    load_dotenv()
    r2_account_id = os.getenv('R2_ACCOUNT_ID')
    r2_access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    r2_secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    r2_bucket_name = os.getenv('R2_BUCKET_NAME')
    print('Loaded Cloudflare R2 variables')

     # === Load environment variables from .env file ===
    dbl_url = os.getenv('DBL_URL')
    print('Loaded youtube environment variables')

    s3 = boto3.client(
        's3', 
        endpoint_url=f'https://{r2_account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=r2_access_key_id,
        aws_secret_access_key=r2_secret_access_key
    )

    response = s3.list_objects_v2(
        Bucket=r2_bucket_name,
        Prefix='incoming/'
    )

    model = video_timestamps.load_whisper_model()
    df_transcript_all= []

    for obj in response.get('Contents', []):
        object = obj['Key']          # e.g. incoming/abc123.mp4
        filename = Path(object).name # e.g. abc123.mp4
        local_path = Path.cwd().parent / "data_preprocessing" / "video_audio" / filename

        # local_path = Path(__file__).resolve().parent.parent / "data_preprocessing" / "video_audio" / filename

        # Ensure the folder exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        s3.download_file(
            r2_bucket_name,
            object, 
            str(local_path)
        )

        id = Path(filename).stem
        video_timestamps.download_video_transcript(id, model)
        df_transcript = video_timestamps.video_transcript_clean(id)
        df_transcript_all.append(df_transcript)

        s3.copy_object(
            r2_bucket_name,
            CopySource={
                'Bucket': r2_bucket_name,
                'Key': object,
            },
            Key=f'processed/{filename}'
        )

        s3.delete_object(
            r2_bucket_name,
            object
        )

    df_transcript_all = pd.concat(df_transcript_all, ignore_index=True)

    # === Insert transcript data into postgreSQL ===
    helper_functions.insert_records_to_postgres(dbl_url, 'sc_yt_video_transcript', df_transcript_all, mode)
    print('Inserted transcript data records into Postgres')


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--mode',
        choices=['append', 'truncate'],
        required=True
    )

    args = parser.parse_args()

    main(args.mode)

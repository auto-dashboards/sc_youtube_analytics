FROM python:3.12-slim

WORKDIR /app

# system dependency (required for yt-dlp + whisper audio handling)
RUN apt-get update && apt-get install -y ffmpeg

# install python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# copy your project into container
COPY . .

# default command (what runs when container starts)
CMD ["python", "data_preprocessing/run_weekly.py", "--mode", "append"]

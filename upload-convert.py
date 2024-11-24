import os
import asyncio
import tempfile
from telethon import TelegramClient
from PIL import Image
import boto3
import subprocess
import sqlite3
import yaml
from datetime import datetime, timezone
import argparse

# Path to the encrypted configuration file
config_path = 'secrets/config.yaml'  # Ensure this file is encrypted with sops

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process Telegram channel media.')
parser.add_argument('--channel-username', required=True, help='Telegram channel username or invite link')
parser.add_argument('--start-date', required=True, help='Start date in YYYY-MM-DD format')
parser.add_argument('--end-date', required=True, help='End date in YYYY-MM-DD format')
parser.add_argument('--s3-key-prefix', required=True, help='S3 key prefix for uploaded files')

args = parser.parse_args()

# Function to load configuration variables from a sops-encrypted YAML file
def load_config(config_path):
    try:
        result = subprocess.run(['sops', '-d', config_path], capture_output=True, text=True, check=True)
        config_data = yaml.safe_load(result.stdout)
        return config_data
    except subprocess.CalledProcessError as e:
        print(f"Failed to decrypt configuration file: {e.stderr}")
        exit(1)

# Load configuration
config = load_config(config_path)

# Parse command line arguments
s3_key_prefix = args.s3_key_prefix
channel_username = args.channel_username
start_date_str = args.start_date
end_date_str = args.end_date

# Configuration Variables
api_id = config['api_id']
api_hash = config['api_hash']
aws_access_key_id = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
s3_endpoint_url = config['s3_endpoint_url']
bucket_name = config['bucket_name']
region_name = config['region_name']
database_path = f"{s3_key_prefix}-processed_messages.db"

# Parse dates and make them timezone-aware (UTC)
start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)

# Initialize Telegram Client
client = TelegramClient('session_name', api_id, api_hash)

def process_image(input_path, output_path):
    """Compresses an image to lower quality."""
    try:
        image = Image.open(input_path)
        image.save(output_path, 'JPEG')
        print(f"Image saved to {output_path}")
    except Exception as e:
        print(f"Failed to process image: {e}")

def process_video(input_path, output_path):
    """Compresses a video using ffmpeg."""
    try:
        # Use ffmpeg to compress video
        command = [
            'ffmpeg', '-y', '-i', input_path,
            '-c:v', 'libx264',
            '-b:v', '500k',
            '-bufsize', '500k',
            '-vf', 'scale=iw:ih',     # Ensures original resolution
            '-c:a', 'copy',
            '-loglevel', 'error',
            output_path
        ]
        subprocess.run(command, check=True)
        print(f"Video saved to {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"ffmpeg error: {e.stderr}")
    except Exception as e:
        print(f"Failed to process video: {e}")

def upload_to_s3(file_path, s3_key):
    """Uploads a file to the specified S3 bucket."""
    s3_client = boto3.client(
        's3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=s3_endpoint_url,
        region_name=region_name,
    )
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")

def init_db():
    """Initializes the SQLite database to keep track of processed messages."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_messages (
            message_id INTEGER PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()

def is_message_processed(message_id):
    """Checks if a message has already been processed."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM processed_messages WHERE message_id = ?', (message_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_message_processed(message_id):
    """Marks a message as processed."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO processed_messages (message_id) VALUES (?)', (message_id,))
    conn.commit()
    conn.close()

async def main():
    await client.start()
    init_db()
    try:
        # Retrieve the channel entity
        channel = await client.get_entity(channel_username)
        print(f"Successfully retrieved channel: {channel.title}")
    except Exception as e:
        print(f"Error retrieving channel: {e}")
        return

    # Iterate over messages from oldest to newest
    async for message in client.iter_messages(channel, reverse=True):
        # Filter messages by date
        if message.date <= start_date:
            continue
        if message.date >= end_date:
            continue
        if is_message_processed(message.id):
            print(f"Message {message.id} already processed. Skipping.")
            continue
        if message.photo or message.video:
            # Download media
            print(f"Processing message {message.id}")
            media = message.photo or message.video
            with tempfile.TemporaryDirectory() as temp_dir:
                input_file = await client.download_media(media, file=temp_dir)
                if not input_file:
                    print(f"Failed to download media for message {message.id}")
                    continue
                if message.photo:
                    # Process image
                    output_file = os.path.join(temp_dir, f'compressed_{message.id}.jpg')
                    process_image(input_file, output_file)
                elif message.video:
                    # Process video
                    output_file = os.path.join(temp_dir, f'compressed_{message.id}.mp4')
                    process_video(input_file, output_file)
                elif message.document:
                    # Process document
                    output_file = os.path.join(temp_dir, f'compressed_{message.id}.{message.document.mime_type.split("/")[1]}')
                    process_image(input_file, output_file)
                else:
                    print(f"Unsupported media type in message {message.id}")
                    continue
                # Upload to S3
                upload_to_s3(output_file, f"{s3_key_prefix}/{os.path.basename(output_file)}")
                # Mark message as processed
                mark_message_processed(message.id)
        else:
            print(f"No media in message {message.id}. Skipping.")
            # Mark message as processed to avoid checking again
            mark_message_processed(message.id)

if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())

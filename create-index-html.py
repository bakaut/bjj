import boto3
import os
import subprocess
import yaml
from urllib.parse import quote
from pathlib import Path
import tempfile
from PIL import Image
import io

config_path = 'secrets/config.yaml'

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

# Configuration Variables
bucket_name = config['bucket_name']
region_name = config['region_name']
aws_access_key_id = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
s3_endpoint_url = config['s3_endpoint_url']
site_base = config['site_base']

s3_client = boto3.client(
    's3', 
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=s3_endpoint_url,
    region_name=region_name,
)

def upload_file_to_s3(bucket_name, file_name, object_name, content_type, s3_client=s3_client):
    """Uploads a file to the specified S3 bucket."""
    try:
        s3_client.upload_file(file_name, bucket_name, object_name, ExtraArgs={'ContentType': content_type})
        print(f"Uploaded {file_name} to s3://bucket_name/{object_name}")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")

def list_subfolders(bucket_name, s3_client=s3_client):
    paginator = s3_client.get_paginator('list_objects_v2')
    subfolders = set()

    for page in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for prefix in page['CommonPrefixes']:
                subfolder = prefix['Prefix'].rstrip('/')
                if subfolder != 'thumbnails':  # Exclude the 'thumbnails' folder
                    subfolders.add(subfolder)
    return sorted(subfolders)

def list_media_files(bucket_name, prefix='', s3_client=s3_client):
    paginator = s3_client.get_paginator('list_objects_v2')
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')
    video_extensions = ('.mp4', '.mov', '.avi', '.wmv', '.flv', '.mkv', '.webm')
    media_extensions = image_extensions + video_extensions
    media_files = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Exclude any files in the 'thumbnails' folder
                if '/thumbnails/' in key or key.startswith('thumbnails/'):
                    continue
                if key.lower().endswith(media_extensions):
                    media_files.append(key)
    return media_files

def generate_thumbnails(bucket_name, media_keys, prefix='', thumbnail_prefix='thumbnails/', s3_client=s3_client):
    """Generates thumbnails for images and videos and uploads them to S3."""
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')
    video_extensions = ('.mp4', '.mov', '.avi', '.wmv', '.flv', '.mkv', '.webm')
    thumbnail_keys = []

    print("Starting thumbnail generation.")

    for key in media_keys:
        # Exclude files in the 'thumbnails' folder
        if '/thumbnails/' in key or key.startswith('thumbnails/'):
            continue

        extension = os.path.splitext(key)[1].lower()
        filename = os.path.basename(key)
        base_filename = os.path.splitext(filename)[0]  # Remove extension from filename
        # Define thumbnail key without double extensions
        thumbnail_key = thumbnail_prefix + key[len(prefix):]
        thumbnail_key = os.path.splitext(thumbnail_key)[0] + '.jpg'  # Ensure single .jpg extension
        thumbnail_keys.append(thumbnail_key)

        # Check if thumbnail already exists
        try:
            s3_client.head_object(Bucket=bucket_name, Key=thumbnail_key)
            # print(f"Thumbnail already exists for {key}, skipping generation.")
            continue  # Skip thumbnail generation
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Thumbnail does not exist, proceed to generate
                pass
            else:
                # Some other error occurred
                print(f"Error checking for thumbnail {thumbnail_key}: {e}")
                continue

        if extension in image_extensions:
            # Generate image thumbnail
            try:
                # Download the image from S3
                obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                img_data = obj['Body'].read()

                # Open the image and create a thumbnail
                with Image.open(io.BytesIO(img_data)) as img:
                    #print(f"Generating thumbnail for {key}")
                    img.thumbnail((150, 150))  # Adjust the thumbnail size as needed

                    # Save the thumbnail to a bytes buffer with compression
                    buffer = io.BytesIO()
                    img.save(buffer, format='JPEG', quality=65)  # Adjust 'quality' for compression
                    buffer.seek(0)

                    # Upload the compressed thumbnail to S3
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=thumbnail_key,
                        Body=buffer,
                        ContentType='image/jpeg'
                    )
                    # print(f"Generated and uploaded compressed thumbnail for {key} as {thumbnail_key}")
            except Exception as e:
                print(f"Error generating thumbnail for {key}: {e}")

        elif extension in video_extensions:
            # Generate video thumbnail using FFmpeg
            #print(f"Generating thumbnail for {key}")
            try:
                # Ensure FFmpeg is installed
                ffmpeg_installed = subprocess.run(['ffmpeg', '-version'], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                if ffmpeg_installed.returncode != 0:
                    print("FFmpeg is not installed or not found in PATH.")
                    continue

                # Create a temporary file to store the video
                with tempfile.NamedTemporaryFile(suffix=extension) as tmp_video:
                    # Download the video from S3
                    s3_client.download_fileobj(bucket_name, key, tmp_video)
                    tmp_video.flush()

                    # Create a temporary file for the thumbnail
                    with tempfile.NamedTemporaryFile(suffix='.jpg') as tmp_thumbnail:
                        # Generate thumbnail at 1 second into the video
                        ffmpeg_command = [
                            'ffmpeg',
                            '-y',  # Automatically overwrite output files
                            '-loglevel', 'error',  # Show only error messages
                            '-ss', '00:00:01.000',
                            '-i', tmp_video.name,
                            '-vframes', '1',
                            '-q:v', '4',  # Adjust 'q:v' for FFmpeg quality (higher is more compressed)
                            tmp_thumbnail.name
                        ]
                        try:
                            subprocess.run(ffmpeg_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        except subprocess.CalledProcessError as e:
                            print(f"FFmpeg failed for {key}: {e.stderr.decode()}")
                            continue

                        # Compress the thumbnail using PIL
                        with Image.open(tmp_thumbnail.name) as img:
                            # Save the image to a bytes buffer with compression
                            img.thumbnail((150, 150))  # Adjust the thumbnail size as needed
                            buffer = io.BytesIO()
                            img.save(buffer, format='JPEG', quality=65)  # Adjust 'quality' for compression
                            buffer.seek(0)

                            # Upload the compressed thumbnail to S3
                            s3_client.put_object(
                                Bucket=bucket_name,
                                Key=thumbnail_key,
                                Body=buffer,
                                ContentType='image/jpeg'
                            )
                        #print(f"Generated and uploaded compressed thumbnail for {key} as {thumbnail_key}")
            except Exception as e:
                print(f"Error generating thumbnail for {key}: {e}")
    return thumbnail_keys

def generate_subfolder_html(subfolder, media_urls, thumbnail_urls, output_file):
    html_content = f'''<!DOCTYPE html>
<html>
<head>
    <title>{subfolder} - Media Gallery</title>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .media {{ margin: 20px; display: inline-block; }}
        img {{ max-width: 150px; height: auto; }}
        a {{ text-decoration: none; color: #000; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
<h1>{subfolder}</h1>
<a href="index.html">Back to Main Index</a>
<hr>
'''

    for thumb_url, media_url in zip(thumbnail_urls, media_urls):
        filename = os.path.basename(media_url)
        html_content += f'''<div class="media">
    <a href="{media_url}" target="_blank">
        <img src="{thumb_url}" alt="{filename}">
    </a>
</div>\n'''

    html_content += '''
</body>
</html>
'''

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Subfolder HTML file '{output_file}' has been generated.")

def generate_index_html(subfolders, output_file='index.html'):
    html_content = '''<!DOCTYPE html>
<html>
<head>
    <title>Media Gallery - Subfolders</title>
    <style>
        body { font-family: Arial, sans-serif; }
        .folder { margin: 20px; }
        a { text-decoration: none; color: #000; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
<h1>Media Gallery</h1>
<ul>
'''
    for subfolder in subfolders:
        subfolder_encoded = quote(subfolder)
        html_content += f'    <li class="folder"><a href="{subfolder_encoded}.html">{subfolder}</a></li>\n'

    html_content += '''
</ul>
</body>
</html>
'''

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Main index HTML file '{output_file}' has been generated.")

def get_public_urls(bucket_name, keys):
    urls = [f'https://{bucket_name}.{site_base}/{key}' for key in keys]
    return urls

def main():
    # Step 1: List subfolders in the S3 bucket's root
    subfolders = list_subfolders(bucket_name)

    # Step 2: Generate the main index.html listing subfolders
    generate_index_html(subfolders)

    # Step 3: Upload the main index.html to S3
    upload_file_to_s3(bucket_name, 'index.html', 'index.html', 'text/html')

    # Step 4: For each subfolder, list media files and generate an HTML page
    for subfolder in subfolders:
        prefix = subfolder + '/'
        media_keys = list_media_files(bucket_name, prefix=prefix)
        media_urls = get_public_urls(bucket_name, media_keys)

        # Step 5: Generate thumbnails for images and videos
        thumbnail_keys = generate_thumbnails(bucket_name, media_keys, prefix=prefix)
        thumbnail_urls = get_public_urls(bucket_name, thumbnail_keys)

        # Step 6: Generate HTML file for the subfolder
        subfolder_html_file = f'{subfolder}.html'
        generate_subfolder_html(subfolder, media_urls, thumbnail_urls, subfolder_html_file)

        # Step 7: Upload the subfolder HTML file to S3
        upload_file_to_s3(bucket_name, subfolder_html_file, subfolder_html_file, 'text/html')

if __name__ == '__main__':
    main()

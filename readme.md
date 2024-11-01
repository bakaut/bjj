# Copy telegram photo and video to s3 
- From certain channel (public or private)
- From certain date range

# Prerequisites
- pip install telethon Pillow boto3 pyyaml
- brew install ffmpeg


# Usage

1. Adjust secret variables `sops secrets/config.yaml`
2. 
```python
python3 upload-convert.py --channel-username invite_link_or_public_username --start-date 2024-08-19 --end-date 2024-08-23 --s3-key-prefix 'prefix'
```

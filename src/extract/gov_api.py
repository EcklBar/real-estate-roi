"""
gov_api.py -- Fetch Israeli real estate data from dirobot.co.il API
and upload raw JSON to MinIO (S3-compatible storage).

Data includes: city summaries, neighborhood stats, price timeseries, 
and individual street-level transactions.
"""

import requests
import json
import os
import io
import time
from datetime import datetime
from urllib.parse import quote
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "nadlanist-raw"

DIROBOT_BASE_URL = "https://api.dirobot.co.il/api/v2"

DIROBOT_HEADERS = {
    "accept": "*/*",
    "origin": "https://www.dirobot.co.il",
    "referer": "https://www.dirobot.co.il/",
    "user-agent": "Mozilla/5.0 (nadlanist-project)",
}
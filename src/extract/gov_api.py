"""
gov_api.py -- Fetch real estate transactions from nadlan.gov.il
and upload raw JSON to MinIO (S3-compatible storage).
"""

import requests
import json
import os
import io
import time
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio

# Load .env file (for local development; in Docker, env vars come from docker-compose)
load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "nadlanist-raw"

NADLAN_GOV_API = "https://www.nadlan.gov.il/Nadlan.REST/Main/GetAssestAndDeals"
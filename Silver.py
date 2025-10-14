import io, json, boto3, pandas as pd
from botocore.client import Config

RAW_BUCKET    = "raw"
SILVER_BUCKET = "silver"
ENDPOINT_URL  = "http://localhost:9000"
ACCESS_KEY    = "minioadmin"
SECRET_KEY    = "minioadmin123"
CAPACITY_TONS = 0.12  # 120 L = 0.12 t par poubelle

# Init client
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4")
)

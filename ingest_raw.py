#!/usr/bin/env python3
"""
ingest_enedis_catalog.py
-------------------------------------------------------------
Télécharge le catalogue Enedis (liste des datasets) et l'envoie
dans le bucket 'raw' de MinIO (ou S3 compatible).
-------------------------------------------------------------
"""

import os
import json
from datetime import datetime
from botocore.client import Config
import boto3
import requests
import logging

# ==========================================================
# CONFIGURATION MINIO
# ==========================================================
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY        = os.getenv("S3_KEY", "minioadmin")
S3_SECRET     = os.getenv("S3_SECRET", "minioadmin")
RAW_BUCKET    = os.getenv("RAW_BUCKET", "raw")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ==========================================================
# INGESTION API ENEDIS
# ==========================================================
def ingest_enedis_catalog():
    url = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"enedis_catalog/date={datetime.utcnow().strftime('%Y/%m/%d')}/enedis_catalog_{ts}.json"

    try:
        # Vérifie et crée le bucket si besoin
        existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}
        if RAW_BUCKET not in existing:
            s3.create_bucket(Bucket=RAW_BUCKET)
            logging.info(f"🪣 Bucket créé : {RAW_BUCKET}")
        else:
            logging.info(f"🪣 Bucket déjà existant : {RAW_BUCKET}")

        # Appel API
        logging.info(f"🌐 Téléchargement du catalogue Enedis : {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        # Upload brut JSON
        data_bytes = json.dumps(resp.json(), indent=2).encode("utf-8")
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=data_bytes,
            ContentType="application/json"
        )
        logging.info(f"✅ Upload réussi → s3://{RAW_BUCKET}/{key} ({len(data_bytes)} octets)")

    except Exception as e:
        logging.error(f"❌ Erreur ingestion Enedis catalog : {e}")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    logging.info("🚀 Ingestion du catalogue Enedis → bucket raw")
    ingest_enedis_catalog()
    logging.info("🏁 Terminé.")

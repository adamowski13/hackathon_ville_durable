#!/usr/bin/env python3
"""
ingest_enedis_multi.py

Télécharge plusieurs datasets Enedis / électrique / infrastructure (CSV ou GeoJSON),
et les envoie dans le bucket RAW défini dans S3_creation.py, en streaming lorsqu’applicable.

Chaque source est configurée avec :
- une clé “name”
- une URL
- un type (csv / geojson)
- un flag “streamable” (True si on peut streamer, sinon on télécharge completement)

Le script gère l’upload idempotent (skip si déjà existant).
"""

import logging
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from S3_creation import s3, RAW_BUCKET

# ----------------------------------------------------------------------
# Config des sources à ingérer
# ----------------------------------------------------------------------
SOURCES = [
    {
        "name": "enedis_residentiel",
        "url": (
            "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/"
            "consommation-annuelle-residentielle-par-adresse/exports/csv"
            "?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
        ),
        "type": "csv",
        "streamable": True
    },
    {
        "name": "poteaux_hta_bt",
        "url": "https://data.enedis.fr/explore/dataset/position-geographique-des-poteaux-hta-et-bt/export/?format=csv",
        "type": "csv",
        "streamable": True
    },
    {
        "name": "reseau_souterrain_hta",
        "url": "https://data.enedis.fr/explore/dataset/reseau-souterrain-hta/export/?format=csv",
        "type": "csv",
        "streamable": True
    },
    {
        "name": "production_par_commune",
        "url": "https://data.enedis.fr/explore/dataset/production-electrique-par-filiere-a-la-maille-commune/export/?format=csv",
        "type": "csv",
        "streamable": True
    },
    {
        "name": "coefficients_profils",
        "url": "https://data.enedis.fr/explore/dataset/coefficients-des-profils/export/?format=csv",
        "type": "csv",
        "streamable": True
    },
]

# ----------------------------------------------------------------------
# Utils S3 / idempotence
# ----------------------------------------------------------------------
def setup_logging(level="INFO"):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

def object_exists(key):
    try:
        s3.head_object(Bucket=RAW_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

# ----------------------------------------------------------------------
# Fonction d’ingestion pour une source
# ----------------------------------------------------------------------
def ingest_source(src):
    ts = datetime.utcnow()
    name = src["name"]
    url = src["url"]
    typ = src.get("type", "csv")
    streamable = src.get("streamable", False)

    ext = ".csv" if typ == "csv" else ".geojson"
    key = ts.strftime(f"api/{name}/date=%Y/%m/%d/{name}_%Y%m%dT%H%M%SZ{ext}")

    if object_exists(key):
        logging.info("Skipped existing %s → s3://%s/%s", name, RAW_BUCKET, key)
        return

    try:
        logging.info("Téléchargement de %s depuis %s", name, url)
        if streamable:
            with requests.get(url, stream=True, timeout=600) as r:
                r.raise_for_status()
                # flux direct vers S3
                s3.upload_fileobj(r.raw, RAW_BUCKET, key)
            logging.info("%s upload terminé en streaming → s3://%s/%s", name, RAW_BUCKET, key)
        else:
            resp = requests.get(url, timeout=300)
            resp.raise_for_status()
            s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=resp.content, ContentType="text/csv")
            logging.info("%s upload terminé (non-stream) → s3://%s/%s", name, RAW_BUCKET, key)
    except Exception as e:
        logging.error("Échec ingestion %s : %s", name, e)

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', default='INFO')
    args = parser.parse_args()

    setup_logging(args.log)
    logging.info("Démarrage de l’ingestion multiple Enedis")

    for src in SOURCES:
        ingest_source(src)
    logging.info("Ingestion multiple terminée")

if __name__ == "__main__":
    main()

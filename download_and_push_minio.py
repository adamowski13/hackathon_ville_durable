import os
import io
import csv
import sys
import time
import json
import gzip
import math
import shutil
import zipfile
import datetime as dt
from pathlib import Path
from urllib.parse import urlencode

import requests
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoie noms colonnes → snake_case simple
    mapping = {}
    for c in df.columns:
        nc = (c.strip()
                .lower()
                .replace("\ufeff", "")      # BOM éventuel
                .replace(" - ", "_")
                .replace("-", "_")
                .replace(" ", "_"))
        # compactage underscores
        while "__" in nc:
            nc = nc.replace("__", "_")
        mapping[c] = nc
    return df.rename(columns=mapping)

def _extract_datetime(df: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    Essaie de retrouver une colonne date/heure, même si son nom varie (ex: 'Date - Heure').
    """
    df = _normalize_columns(df)
    # 1) noms courants
    candidates_exact = ["date_heure", "dateheure", "datetime", "date_time", "date_et_heure"]
    for k in candidates_exact:
        if k in df.columns:
            return pd.to_datetime(df[k], errors="coerce"), k

    # 2) colonnes qui contiennent à la fois 'date' et 'heure'
    for c in df.columns:
        lc = c.lower()
        if ("date" in lc) and ("heure" in lc):
            return pd.to_datetime(df[c], errors="coerce"), c

    # 3) 'date' et 'heure' séparées
    if "date" in df.columns and "heure" in df.columns:
        s = pd.to_datetime(df["date"].astype(str) + " " + df["heure"].astype(str), errors="coerce")
        return s, "date_heure"

    # 4) dernier recours: 1ère colonnne qui se parse
    for c in df.columns:
        s = pd.to_datetime(df[c], errors="coerce")
        if s.notna().sum() > 0:
            return s, c

    raise ValueError("Aucune colonne date/heure identifiable dans ce CSV.")

# ------------------ Config ------------------
load_dotenv(".env")
ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET = os.getenv("MINIO_BUCKET", "smartcity-energy")
PREFIX = os.getenv("MINIO_PREFIX", "raw/")

GET_RTE_NATIONAL_TR = os.getenv("GET_RTE_NATIONAL_TR", "true").lower() == "true"
GET_ODRE_CONSO_QJ   = os.getenv("GET_ODRE_CONSO_QJ", "true").lower() == "true"
GET_ENEDIS_JOUR_CATEG = os.getenv("GET_ENEDIS_JOUR_CATEG", "true").lower() == "true"

local_dl = Path("downloads")
local_dl.mkdir(exist_ok=True)

client = Minio(ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)

def ensure_bucket(name: str):
    found = client.bucket_exists(name)
    if not found:
        client.make_bucket(name)
        print(f"[MinIO] Bucket créé: {name}")
    else:
        print(f"[MinIO] Bucket existant: {name}")

def upload_file(local_path: Path, bucket: str, key: str):
    client.fput_object(bucket, key, str(local_path))
    print(f"[MinIO] Upload OK: s3://{bucket}/{key}")

def save_and_push(df: pd.DataFrame, fname: str):
    p = local_dl / fname
    df.to_csv(p, index=False)
    upload_file(p, BUCKET, f"{PREFIX}{fname}")

# ------------------ Download helpers ------------------

def download_csv(url: str, params: dict | None = None) -> pd.DataFrame:
    if params:
        url = url + ("&" if "?" in url else "?") + urlencode(params)
    print(f"[GET] {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    content = r.content.decode("utf-8", errors="ignore")

    # 1ère passe: séparateur virgule (par défaut)
    df = pd.read_csv(io.StringIO(content))
    # Si une seule colonne ET on voit des ';' → relire avec sep=';'
    if df.shape[1] == 1 and (';' in df.columns[0] or ';' in content[:500]):
        df = pd.read_csv(io.StringIO(content), sep=';', engine='python')

    return df


# ------------------ Datasets ------------------
def get_rte_national_tr():
    url = "https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true"
    df = download_csv(url)
    print("[RTE] Colonnes CSV brutes:", list(df.columns))

    df = _normalize_columns(df)
    dt_series, _ = _extract_datetime(df)
    df["date_heure_std"] = dt_series
    df = df[df["date_heure_std"].notna()].copy()
    df = df.sort_values("date_heure_std")

    cutoff = df["date_heure_std"].max() - pd.Timedelta(days=30)
    df_small = df[df["date_heure_std"] >= cutoff].copy()

    save_and_push(df_small, "rte_eco2mix_national_tr_last30d.csv")

def get_odre_conso_qj():
    """
    ODRÉ – Consommation quotidienne brute (élec demi-heure, gaz heure)
    Dataset: consommation-quotidienne-brute
    Export CSV:
      https://odre.opendatasoft.com/explore/dataset/consommation-quotidienne-brute/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true
    """
    url = "https://odre.opendatasoft.com/explore/dataset/consommation-quotidienne-brute/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true"
    df = download_csv(url)
    # Filtrer électricité uniquement et garder dernières 8 semaines
    if "energie" in df.columns:
        df = df[df["energie"].str.contains("Electric", case=False, na=False)]
    # Harmoniser date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        cutoff = df["date"].max() - pd.Timedelta(days=56)
        df = df[df["date"] >= cutoff].copy()
    save_and_push(df, "odre_consommation_quotidienne_elec_last8w.csv")

def get_enedis_jour_categorie():
    """
    Enedis – Consommation d'électricité journalière par catégorie client
    Dataset: bilan-electrique-transpose
    Export CSV:
      https://data.enedis.fr/explore/dataset/bilan-electrique-transpose/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true
    """
    url = "https://data.enedis.fr/explore/dataset/bilan-electrique-transpose/download/?format=csv&timezone=Europe/Paris&use_labels_for_header=true"
    df = download_csv(url)
    # Garde 6 mois récents et colonnes utiles si présentes
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        cutoff = df["date"].max() - pd.Timedelta(days=180)
        df = df[df["date"] >= cutoff].copy()
    cols_pref = [c for c in df.columns if c.lower() in {"date","categorie_client","consommation_mwh","consommation"}]
    if cols_pref:
        df = df[cols_pref + [c for c in df.columns if c not in cols_pref]]
    save_and_push(df, "enedis_conso_journaliere_categorie_last6m.csv")

def main():
    ensure_bucket(BUCKET)
    if GET_RTE_NATIONAL_TR:
        get_rte_national_tr()
    if GET_ODRE_CONSO_QJ:
        get_odre_conso_qj()
    if GET_ENEDIS_JOUR_CATEG:
        get_enedis_jour_categorie()
    print("\nFini. Fichiers poussés dans MinIO sous le préfixe:", PREFIX)

if __name__ == "__main__":
    main()

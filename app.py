
import os, io
import pandas as pd
from datetime import timedelta
import streamlit as st
from minio import Minio
from dotenv import load_dotenv
from joblib import load
import matplotlib.pyplot as plt

st.set_page_config(page_title="SmartEnergy Dashboard", layout="wide")

# --------------------- Config / Connexion MinIO ---------------------
load_dotenv('.env')
ENDPOINT = os.getenv('MINIO_ENDPOINT','localhost:9000')
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY','minioadmin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY','minioadmin')
SECURE = os.getenv('MINIO_SECURE','false').lower()=='true'
BUCKET = os.getenv('MINIO_BUCKET','smartcity-energy')
PREFIX = os.getenv('MINIO_PREFIX','raw/')
MODELS_PREFIX = PREFIX.replace('raw/','models/')

client = Minio(ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)

def read_csv_from_minio(bucket, key):
    resp = client.get_object(bucket, key)
    try:
        data = resp.read()
        sample = data[:500].decode('utf-8', errors='ignore')
        sep = ';' if (';' in sample and ',' not in sample.split('\n')[0]) else ','
        return pd.read_csv(io.BytesIO(data), sep=sep)
    finally:
        resp.close(); resp.release_conn()

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (df.columns.str.strip()
                  .str.lower()
                  .str.replace(" - ", "_")
                  .str.replace("-", "_")
                  .str.replace(" ", "_"))
    return df

def detect_time_col(df: pd.DataFrame) -> str:
    for c in ['date_heure_std','date_heure','date_et_heure','datetime','date_time']:
        if c in df.columns:
            return c
    if 'date' in df.columns and 'heure' in df.columns:
        df['date_heure_std'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['heure'].astype(str), errors='coerce')
        return 'date_heure_std'
    for c in df.columns:
        try:
            s = pd.to_datetime(df[c], errors='coerce')
            if s.notna().sum() > 0:
                df['date_heure_std'] = s
                return 'date_heure_std'
        except Exception:
            pass
    raise ValueError("Impossible de détecter la colonne temporelle.")

def detect_target_col(df: pd.DataFrame) -> str:
    candidates = [c for c in df.columns if 'consommation' in c]
    if candidates:
        return candidates[0]
    for c in ['valeur','value']:
        if c in df.columns:
            return c
    numerics = df.select_dtypes('number').columns.tolist()
    if numerics:
        return numerics[-1]
    raise ValueError("Impossible de détecter la colonne cible (consommation).")

# --------------------- Sidebar ---------------------
st.sidebar.title("⚙️ Options")
forecast_hours = st.sidebar.select_slider("Horizon de prévision", options=[6,12,24], value=24)
peak_threshold = st.sidebar.number_input("Seuil d'alerte pic (MW)", value=60000.0, step=500.0, format="%.0f")
rte_key = PREFIX + "rte_eco2mix_national_tr_last30d.csv"
model_key = MODELS_PREFIX + "rf_baseline.joblib"

st.title("⚡ SmartEnergy – Consommation & Prévision courte échéance")

# --------------------- Charge RTE ---------------------
try:
    df = read_csv_from_minio(BUCKET, rte_key)
except Exception as e:
    st.error(f"Erreur de lecture du CSV RTE depuis MinIO : {e}")
    st.stop()

df = normalize_cols(df)
try:
    time_col = detect_time_col(df)
except Exception:
    if 'date_-_heure' in df.columns:
        df['date_heure_std'] = pd.to_datetime(df['date_-_heure'], errors='coerce')
        time_col = 'date_heure_std'
    else:
        st.error("Impossible d'identifier la colonne date/heure. Vérifie le notebook/ingestion.")
        st.stop()

try:
    target_col = detect_target_col(df)
except Exception as e:
    st.error(str(e)); st.stop()

df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
df = df.dropna(subset=[time_col, target_col]).sort_values(time_col)
df['hour'] = df[time_col].dt.hour
df['dow'] = df[time_col].dt.dayofweek
df['is_weekend'] = df['dow'].isin([5,6]).astype(int)

# --------------------- Graph historique ---------------------
st.subheader("Historique – 30 derniers jours")
hist = df[[time_col, target_col]].set_index(time_col)
st.line_chart(hist)

# --------------------- Chargement du modèle ---------------------
model = None
try:
    resp = client.get_object(BUCKET, model_key)
    by = io.BytesIO(resp.read())
    resp.close(); resp.release_conn()
    model = load(by)
    st.success("Modèle chargé depuis MinIO : rf_baseline.joblib")
except Exception:
    st.info("Modèle introuvable dans MinIO. Lance d'abord le notebook pour l'entraîner et le sauvegarder.")
    
# --------------------- Prévision ---------------------
if model is not None:
    periods = int(forecast_hours * 60 / 15)  # pas 15 min
    last_ts = df[time_col].max()
    future_idx = pd.date_range(last_ts + pd.Timedelta(minutes=15), periods=periods, freq="15T")
    fut = pd.DataFrame({time_col: future_idx})
    fut['hour'] = fut[time_col].dt.hour
    fut['dow'] = fut[time_col].dt.dayofweek
    fut['is_weekend'] = fut['dow'].isin([5,6]).astype(int)

    feats = ['hour','dow','is_weekend']
    fut['prediction'] = model.predict(fut[feats])

    st.subheader(f"Prévision prochaine(s) {forecast_hours} h (pas 15 min)")
    st.line_chart(fut.set_index(time_col)['prediction'])

    nb_peaks = int((fut['prediction'] > peak_threshold).sum())
    if nb_peaks > 0:
        st.warning(f"⚠️ {nb_peaks} intervalles dépassent le seuil de {peak_threshold:.0f} MW.")
    else:
        st.success("Aucun dépassement de seuil prévu sur l'horizon sélectionné.")

<<<<<<< HEAD
import streamlit as st
import pandas as pd
import numpy as np
import requests
import joblib
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Prévisions Électricité IDF", layout="wide")

# --- Charger le modèle ---
model = joblib.load("random_forest_meteo_only.pkl")

# --- Sidebar ---
st.sidebar.header("Paramètres")
city = st.sidebar.text_input("Ville", "Paris")
days = st.sidebar.slider("Nombre de jours de prévision", 1, 7, 7)

# --- Récupérer les données météo ---
API_KEY = "ad835a8d9bf14e938e0204738251610"

def get_weather(city, days=7):
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={city}&days={days}&aqi=no&alerts=no"
    response = requests.get(url)
    data = response.json()
    
    forecast = []
    for day in data['forecast']['forecastday']:
        forecast.append({
            "date": day["date"],
            "Pluie_mm": day["day"]["totalprecip_mm"],
            "Tn_Min": day["day"]["mintemp_c"],
            "Tx_Max": day["day"]["maxtemp_c"],
            "T_Moyenne": day["day"]["avgtemp_c"],
            "Vent_Moyen": day["day"]["maxwind_kph"],  # ou avg si dispo
            "Vent_Max": day["day"]["maxwind_kph"]
        })
    return pd.DataFrame(forecast)

weather_df = get_weather(city, days)

st.subheader(f"Prévisions météo pour {city} ({days} jours)")
st.dataframe(weather_df)

# --- Préparation pour le modèle ---
weather_df["date"] = pd.to_datetime(weather_df["date"])
weather_df["annee"] = weather_df["date"].dt.year
weather_df["mois"] = weather_df["date"].dt.month
weather_df["jour"] = weather_df["date"].dt.day
weather_df["jour_semaine"] = weather_df["date"].dt.weekday

# Supprimer les lags (pas disponibles)
X_pred = weather_df.drop(columns=["date"])

# --- Prédictions ---
y_pred = model.predict(X_pred)
weather_df["elec_MW_pred"] = y_pred

# --- Graphiques interactifs ---
st.subheader("Prévision de consommation électrique (interactive)")

# 1️⃣ Courbe consommation
fig1 = px.line(weather_df, x="date", y="elec_MW_pred", markers=True,
               title="Prévision consommation électrique",
               labels={"elec_MW_pred": "Électricité (MW)", "date": "Date"})
st.plotly_chart(fig1, use_container_width=True)

# 2️⃣ Graphique combiné météo vs consommation
st.subheader("Météo vs consommation prévue")
fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=weather_df["date"], y=weather_df["elec_MW_pred"],
                          mode='lines+markers', name="Élec MW", line=dict(color='firebrick')))
fig2.add_trace(go.Bar(x=weather_df["date"], y=weather_df["T_Moyenne"],
                      name="Température moyenne", marker_color='skyblue', opacity=0.5, yaxis='y2'))

fig2.update_layout(
    title="Consommation électrique vs Pluie",
    xaxis_title="Date",
    yaxis=dict(title="Élec MW"),
    yaxis2=dict(title="Pluie mm", overlaying='y', side='right'),
    legend=dict(x=0.02, y=0.98)
)
st.plotly_chart(fig2, use_container_width=True)

# 3️⃣ Heatmap températures
st.subheader("Température (min/max/moy) sur la semaine")
temp_df = weather_df.melt(id_vars="date", value_vars=["Tn_Min","Tx_Max","T_Moyenne"],
                          var_name="Type", value_name="Température °C")
fig3 = px.imshow(temp_df.pivot(index="Type", columns="date", values="Température °C"),
                 text_auto=True, aspect="auto", color_continuous_scale="RdYlBu_r")
st.plotly_chart(fig3, use_container_width=True)

# --- Statistiques rapides ---
st.subheader("Statistiques sur la semaine")
st.write(f"Consommation moyenne : {weather_df['elec_MW_pred'].mean():.2f} MW")
st.write(f"Consommation max : {weather_df['elec_MW_pred'].max():.2f} MW")
st.write(f"Consommation min : {weather_df['elec_MW_pred'].min():.2f} MW")
st.write(f"Pluie totale : {weather_df['Pluie_mm'].sum():.2f} mm")
st.write(f"Température moyenne : {weather_df['T_Moyenne'].mean():.2f} °C")
st.write(f"Température max : {weather_df['Tx_Max'].max():.2f} °C")
st.write(f"Température min : {weather_df['Tn_Min'].min():.2f} °C")
=======

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
>>>>>>> 7ce8ab9a9d62ff7ce96176a26f292715a4de2a7b

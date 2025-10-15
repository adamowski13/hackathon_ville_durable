# =========================================
# data_cleaning.py
# Nettoyage des données consommation + météo
# =========================================

import pandas as pd

# --- PARAMÈTRES GÉNÉRAUX ---
TZ = "Europe/Paris"
PATH_CONS = "data/consommation-idf.csv"
PATH_METEO = "data/meteo75.csv"
OUTPUT_PATH = "cleaned_data/idf_conso_meteo_clean.parquet"

# --- 1. CHARGEMENT ET NETTOYAGE CONSOMMATION ---
def load_clean_consommation(path):
    cons = pd.read_csv(path, sep=";", encoding="utf-8")
    cons["Date - Heure"] = pd.to_datetime(cons["Date - Heure"], utc=True)
    cons = cons.set_index("Date - Heure").sort_index()
    
    # Conversion fuseau + suppression info tz pour compatibilité
    cons.index = cons.index.tz_convert(TZ).tz_localize(None)
    
    cols = [
        "Consommation brute gaz (MW PCS 0°C) - NaTran",
        "Consommation brute gaz (MW PCS 0°C) - Teréga",
        "Consommation brute gaz totale (MW PCS 0°C)",
        "Consommation brute électricité (MW) - RTE",
        "Consommation brute totale (MW)"
    ]
    cons = cons[cols].apply(pd.to_numeric, errors="coerce").dropna(how="all")
    cons_daily = cons.resample("D").mean()
    cons_daily.columns = [
        "gaz_NaTran_MW", "gaz_Terega_MW", "gaz_total_MW",
        "elec_MW", "conso_totale_MW"
    ]
    return cons_daily
# --- 5. AJOUT DES LAG FEATURES ---
def add_lag_features(df):
    """
    Ajoute des features temporelles dérivées :
    - lags : consommation d'hier, d'il y a 7 jours
    - rolling mean : moyenne sur 7 jours
    """
    df = df.copy()
    
    # Lags sur consommation totale
    df['conso_totale_MW_t-1'] = df['conso_totale_MW'].shift(1)
    df['conso_totale_MW_t-7'] = df['conso_totale_MW'].shift(7)
    
    # Moyenne glissante sur 7 jours
    df['conso_totale_MW_roll7'] = df['conso_totale_MW'].rolling(window=7).mean()
    
    # Optionnel : lags pour électricité et gaz si besoin
    df['elec_MW_t-1'] = df['elec_MW'].shift(1)
    df['gaz_total_MW_t-1'] = df['gaz_total_MW'].shift(1)
    
    # Supprime les premières lignes contenant des NaN dus aux lags
    df = df.dropna()
    
    return df


# --- 2. CHARGEMENT ET NETTOYAGE MÉTÉO ---
def load_clean_meteo(path):
    meteo = pd.read_csv(path, sep=",", encoding="utf-8")
    meteo["Date"] = pd.to_datetime(meteo["Date"])
    
    cols = [
        "Date", "Pluie_mm", "Tn_Min", "Tx_Max",
        "T_Moyenne", "Vent_Moyen", "Vent_Max"
    ]
    meteo = meteo[cols]
    
    meteo_day = (
        meteo.groupby("Date")
        .agg({
            "Pluie_mm": "sum",
            "Tn_Min": "min",
            "Tx_Max": "max",
            "T_Moyenne": "mean",
            "Vent_Moyen": "mean",
            "Vent_Max": "max"
        })
    )
    meteo_day = meteo_day.replace([-9999, 9999], pd.NA).interpolate(method="time")
    return meteo_day

# --- 3. FUSION ---
def merge_datasets(cons_df, meteo_df):
    merged = cons_df.join(meteo_df, how="inner").dropna(subset=["conso_totale_MW"])
    return merged

# --- 4. MAIN ---
if __name__ == "__main__":
    print("Chargement et nettoyage consommation...")
    cons_df = load_clean_consommation(PATH_CONS)
    
    print("Chargement et nettoyage météo...")
    meteo_df = load_clean_meteo(PATH_METEO)
    
    print("Fusion des deux jeux de données...")
    merged = merge_datasets(cons_df, meteo_df)
    
    print("Aperçu du dataset final :")
    print(merged.head())
    print("\nDimensions :", merged.shape)

    # print("Ajout des lag features...")
    # merged = add_lag_features(merged)
    
    # Export
    merged.to_parquet(OUTPUT_PATH)
    merged.to_csv(OUTPUT_PATH.replace(".parquet", ".csv"))
    
    print(f"\n✅ Fichiers enregistrés dans {OUTPUT_PATH}")

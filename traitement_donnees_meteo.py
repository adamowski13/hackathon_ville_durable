import pandas as pd

# Charger le fichier Parquet
df = pd.read_parquet('data/meteo75.parquet')

print(f"Nombre de lignes chargées : {len(df)}")

# Conversion de la colonne date
df['AAAAMMJJ'] = pd.to_datetime(df['AAAAMMJJ'], errors='coerce')

# Filtrer pour les années > 2018
df = df[df['AAAAMMJJ'].dt.year > 2018]

# Renommer les colonnes
def rename_columns(df):
    rename_dict = {
        'NUM_POSTE': 'Station_Num',
        'NOM_USUEL': 'Station_Nom',
        'LAT': 'Latitude',
        'LON': 'Longitude',
        'ALTI': 'Altitude',
        'AAAAMMJJ': 'Date',
        # Précipitations
        'RR': 'Pluie_mm',
        'QRR': 'Qualite_Pluie',
        # Température minimale
        'TN': 'Tn_Min',
        'QTN': 'Qualite_Tn_Min',
        'HTN': 'Tn_Horaire',
        'QHTN': 'Qualite_Tn_Horaire',
        # Température maximale
        'TX': 'Tx_Max',
        'QTX': 'Qualite_Tx_Max',
        'HTX': 'Tx_Horaire',
        'QHTX': 'Qualite_Tx_Horaire',
        # Température moyenne
        'TM': 'T_Moyenne',
        'QTM': 'Qualite_T_Moyenne',
        'TNTXM': 'Tn_Tx_Moyenne',
        'QTNTXM': 'Qualite_Tn_Tx_Moyenne',
        # Température amplitude / sol
        'TAMPLI': 'Amplitude_Temp',
        'QTAMPLI': 'Qualite_Amplitude_Temp',
        'TNSOL': 'Tn_Sol',
        'QTNSOL': 'Qualite_Tn_Sol',
        'TN50': 'Tn_Min_Record',
        'QTN50': 'Qualite_Tn_Min_Record',
        # Vent
        'DG': 'Vent_Direction',
        'QDG': 'Qualite_Vent_Direction',
        'FFM': 'Vent_Moyen',
        'QFFM': 'Qualite_Vent_Moyen',
        'FF2M': 'Vent_2m',
        'QFF2M': 'Qualite_Vent_2m',
        'FXY': 'Vent_Max',
        'QFXY': 'Qualite_Vent_Max',
        'DXY': 'Direction_Vent_Max',
        'QDXY': 'Qualite_Direction_Vent_Max',
        'HXY': 'Hauteur_Vent_Max',
        'QHXY': 'Qualite_Hauteur_Vent_Max',
        'FXI': 'Vent_I',
        'QFXI': 'Qualite_Vent_I',
        'DXI': 'Direction_Vent_I',
        'QDXI': 'Qualite_Direction_Vent_I',
        'HXI': 'Hauteur_Vent_I',
        'QHXI': 'Qualite_Hauteur_Vent_I',
        'FXI2': 'Vent_I2',
        'QFXI2': 'Qualite_Vent_I2',
        'DXI2': 'Direction_Vent_I2',
        'QDXI2': 'Qualite_Direction_Vent_I2',
        'HXI2': 'Hauteur_Vent_I2',
        'QHXI2': 'Qualite_Hauteur_Vent_I2',
        'FXI3S': 'Vent_I3S',
        'QFXI3S': 'Qualite_Vent_I3S',
        'DXI3S': 'Direction_Vent_I3S',
        'QDXI3S': 'Qualite_Direction_Vent_I3S',
        'HXI3S': 'Hauteur_Vent_I3S',
        'QHXI3S': 'Qualite_Hauteur_Vent_I3S',
        # Pluie/évaporation
        'DRR': 'Pluie_Records',
        'QDRR': 'Qualite_Pluie_Records'
    }
    return df.rename(columns=rename_dict)

df = rename_columns(df)
# df = df.rename(columns=rename_dict)

# Export CSV
df.to_csv('data/meteo75.csv', index=False)

print("✅ CSV généré avec colonnes renommées et filtrage après 2018.")

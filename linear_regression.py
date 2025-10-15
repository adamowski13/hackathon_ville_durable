# =========================================
# linear_regression_v2.py
# Modèle de prédiction avec features temporelles + visualisation
# =========================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

# --- PARAMÈTRES ---
DATA_PATH = "cleaned_data/idf_conso_meteo_clean.parquet"

# --- 1. CHARGEMENT DES DONNÉES ---
df = pd.read_parquet(DATA_PATH)

# --- 2. FEATURES TEMPORELLES ---
df["jour_semaine"] = df.index.dayofweek  # 0 = lundi
df["mois"] = df.index.month

# Encodage cyclique pour la saisonnalité
df["sin_jour"] = np.sin(2 * np.pi * df["jour_semaine"] / 7)
df["cos_jour"] = np.cos(2 * np.pi * df["jour_semaine"] / 7)
df["sin_mois"] = np.sin(2 * np.pi * df["mois"] / 12)
df["cos_mois"] = np.cos(2 * np.pi * df["mois"] / 12)

# --- 3. FEATURES & TARGET ---
X = df[[
    "Pluie_mm", "Tn_Min", "Tx_Max", "T_Moyenne",
    "Vent_Moyen", "Vent_Max",
    "sin_jour", "cos_jour", "sin_mois", "cos_mois"
]]
y = df["conso_totale_MW"]

# --- 4. SPLIT TRAIN/TEST ---
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)

# --- 5. MODÈLE ---
model = LinearRegression()
model.fit(X_train, y_train)

# --- 6. PRÉDICTIONS ---
y_pred = model.predict(X_test)

# --- 7. ÉVALUATION ---
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("=== Évaluation du modèle v2 ===")
print(f"MAE  : {mae:.2f}")
print(f"RMSE : {rmse:.2f}")
print(f"R²   : {r2:.3f}")

# --- 8. VISUALISATION ---
results = pd.DataFrame({
    "y_true": y_test,
    "y_pred": y_pred
}, index=y_test.index)

plt.figure(figsize=(12,6))
plt.plot(results.index, results["y_true"], label="Consommation réelle", linewidth=2)
plt.plot(results.index, results["y_pred"], label="Prédiction", linewidth=2, alpha=0.7)
plt.title("Prévision de la consommation électrique en IDF")
plt.xlabel("Date")
plt.ylabel("Consommation (MW)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


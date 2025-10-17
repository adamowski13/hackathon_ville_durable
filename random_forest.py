import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import matplotlib.pyplot as plt
import seaborn as sns

# ==========================
# 1. Chargement des données
# ==========================
data_path = "cleaned_data/idf_conso_meteo_clean.csv"
data = pd.read_csv(data_path)

# Renommer colonne date si besoin
if "Unnamed: 0" in data.columns:
    data = data.rename(columns={"Unnamed: 0": "date"})

# Conversion en datetime
data["date"] = pd.to_datetime(data["date"], errors="coerce")
data = data.dropna(subset=["date"])

# Features temporelles
data["annee"] = data["date"].dt.year
data["mois"] = data["date"].dt.month
data["jour"] = data["date"].dt.day
data["jour_semaine"] = data["date"].dt.weekday

# Supprimer la colonne date
data = data.drop(columns=["date"])

# Remplacer les NaN par la moyenne
data = data.fillna(data.mean(numeric_only=True))

# ==========================
# 2. Définition de la cible
# ==========================
target_col = "elec_MW"
if target_col not in data.columns:
    raise ValueError(f"Colonne cible '{target_col}' introuvable")

# Supprimer toutes les colonnes liées aux lags ou à l’électricité passée
cols_to_drop = [c for c in data.columns if "t-1" in c or "t-7" in c or "roll" in c]
X = data.drop(columns=[target_col] + cols_to_drop)
y = data[target_col]

# ==========================
# 3. Séparation des données
# ==========================
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ==========================
# 4. Recherche d’hyperparamètres
# ==========================
param_grid = {
    "n_estimators": [100, 200],
    "max_depth": [10, 20, None],
    "min_samples_split": [2, 5],
    "min_samples_leaf": [1, 2],
}

rf = RandomForestRegressor(random_state=42, n_jobs=-1)
grid_search = GridSearchCV(
    estimator=rf,
    param_grid=param_grid,
    cv=3,
    n_jobs=-1,
    scoring="r2",
    verbose=1
)
grid_search.fit(X_train, y_train)

best_rf = grid_search.best_estimator_
print("Meilleurs hyperparamètres :", grid_search.best_params_)

# ==========================
# 5. Évaluation du modèle
# ==========================
y_pred = best_rf.predict(X_test)
print(f"MAE  : {mean_absolute_error(y_test, y_pred):.2f}")
print(f"RMSE : {np.sqrt(mean_squared_error(y_test, y_pred)):.2f}")
print(f"R²   : {r2_score(y_test, y_pred):.3f}")

# ==========================
# 6. Importance des features
# ==========================
importances = pd.DataFrame({
    "Feature": X.columns,
    "Importance": best_rf.feature_importances_
}).sort_values(by="Importance", ascending=False)

plt.figure(figsize=(10, 6))
sns.barplot(x="Importance", y="Feature", data=importances)
plt.title("Importance des variables (Random Forest, météo uniquement)")
plt.tight_layout()
plt.show()

# ==========================
# 7. Sauvegarde du modèle
# ==========================
joblib.dump(best_rf, "random_forest_meteo_only.pkl")
print("✅ Modèle sauvegardé dans 'random_forest_meteo_only.pkl'")

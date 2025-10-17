import requests
import pandas as pd
import time
import os

# =========================================
# 1️⃣  CONFIGURATION GLOBALE
# =========================================

DATASETS = {
    "consommation_commune": "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-electrique-par-secteur-dactivite-commune/records",
    "bilan_electrique": "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/bilan-electrique/records",
    "conso_residentielle": "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-residentielle-par-adresse/records"
}

ANNEES = ["2021", "2022", "2023"]

# Communes Paris (ID 75)
PARIS_COMMUNES = ["Paris"]  # ici tu peux ajouter d'autres communes si nécessaire

# Crée le dossier de sauvegarde si inexistant
os.makedirs("data", exist_ok=True)

# =========================================
# 2️⃣  FONCTION D’EXTRACTION ENEDIS
# =========================================
def fetch_enedis_data(dataset_url, params, filter_commune=None):
    """Récupère les données Enedis avec pagination et filtre sur la commune"""
    all_records = []
    params["offset"] = 0
    limit = params.get("limit", 1000)
    page = 0

    while True:
        page += 1
        print(f"➡️  Page {page} (offset={params['offset']})...")
        r = requests.get(dataset_url, params=params)
        if r.status_code != 200:
            print(f"❌ Erreur {r.status_code}: {r.text}")
            break

        data = r.json()
        records = data.get("results", [])
        if not records:
            break

        # Filtre sur la commune si demandé
        if filter_commune:
            records = [rec for rec in records if rec.get("fields", {}).get("nom_commune") in filter_commune]

        all_records.extend(records)
        params["offset"] += limit
        time.sleep(0.2)

        # if params["offset"] + limit > 10000:  # Limite API Enedis
            # print("⚠️  Limite de 10k atteinte pour ce filtre.")
        #     break

    if all_records:
        # Extraction du champ 'fields' qui contient les vraies données
        df = pd.DataFrame([rec.get("fields", {}) for rec in all_records])
        return df
    else:
        return pd.DataFrame()

# =========================================
# 3️⃣  PIPELINE PRINCIPAL
# =========================================
def main():
    all_datasets = []

    for name, url in DATASETS.items():
        for annee in ANNEES:
            print(f"\n🚀 Extraction {name} - année {annee} - Paris uniquement")
            params = {"limit": 1000, "refine.annee": annee}
            df = fetch_enedis_data(url, params, filter_commune=PARIS_COMMUNES)
            if not df.empty:
                df["dataset"] = name
                df["annee"] = annee
                all_datasets.append(df)

    # Fusionne tous les datasets Enedis
    if all_datasets:
        enedis_df = pd.concat(all_datasets, ignore_index=True)
        print(f"\n✅ {len(enedis_df)} lignes Enedis Paris consolidées.")
        enedis_df.to_csv("/data/enedis_paris.csv", index=False)
        print(f"💾 Données sauvegardées dans 'data/enedis_paris.csv'")
    else:
        print("⚠️ Aucune donnée Enedis Paris récupérée.")

# =========================================
# 4️⃣  EXECUTION
# =========================================
if __name__ == "__main__":
    print("🔄 Démarrage du pipeline Enedis Paris...")
    main()
    print("🎯 Pipeline terminé !")

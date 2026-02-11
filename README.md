# 🌍 Hackathon Ville Durable

Projet de prédiction de la consommation énergétique pour une ville durable utilisant des données météorologiques et de consommation électrique.

## 📋 Description

Ce projet développé dans le cadre d'un hackathon vise à analyser et prédire la consommation énergétique en combinant des données météorologiques et des données de consommation électrique d'Enedis. L'objectif est de contribuer à une gestion plus efficace de l'énergie dans les villes durables.

## 🏗️ Architecture

Le projet suit une architecture de data pipeline en plusieurs couches :

- **Bronze Layer** : Ingestion des données brutes depuis les sources (API Enedis, données météo)
- **Silver Layer** : Nettoyage et transformation des données
- **Gold Layer** : Modèles de machine learning et analyses

### Technologies utilisées

- **Python** : Langage principal
- **MinIO/S3** : Stockage objet pour le data lake
- **Docker** : Containerisation de l'infrastructure
- **Machine Learning** : Régression linéaire et Random Forest
- **Jupyter Notebook** : Analyse exploratoire des données

## 📁 Structure du projet

```
hackathon_ville_durable/
├── S3_creation.py                    # Création et configuration des buckets S3/MinIO
├── api_enedis.py                     # Interface avec l'API Enedis
├── download_and_push_minio.py        # Téléchargement et upload vers MinIO
├── ingest_raw.py                     # Ingestion des données brutes (Bronze)
├── clean_data.py                     # Nettoyage des données
├── Silver.py                         # Transformation des données (Silver layer)
├── traitement_donnees_conso.py       # Traitement spécifique consommation
├── traitement_donnees_meteo.py       # Traitement spécifique météo
├── linear_regression.py              # Modèle de régression linéaire
├── random_forest.py                  # Modèle Random Forest
├── app.py                            # Application principale
├── eda_template.ipynb                # Notebook d'analyse exploratoire
├── docker-compose.yml                # Configuration Docker
├── requirements.txt                  # Dépendances Python
└── .env                              # Variables d'environnement
```

## 🚀 Installation

### Prérequis

- Python 3.8+
- Docker et Docker Compose
- Compte API Enedis (pour accéder aux données de consommation)

### Installation des dépendances

```bash
# Cloner le repository
git clone https://github.com/adamowski13/hackathon_ville_durable.git
cd hackathon_ville_durable

# Installer les dépendances Python
pip install -r requirements.txt
```

### Configuration de l'environnement

1. Créer un fichier `.env` à la racine du projet avec vos configurations :

```env
# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=votre_access_key
MINIO_SECRET_KEY=votre_secret_key

# API Enedis
ENEDIS_API_KEY=votre_api_key

# Autres configurations
```

2. Lancer l'infrastructure avec Docker :

```bash
docker-compose up -d
```

## 💻 Utilisation

### 1. Création des buckets S3/MinIO

```bash
python S3_creation.py
```

### 2. Ingestion des données brutes

```bash
# Télécharger et stocker les données depuis les API
python download_and_push_minio.py

# Ingérer les données dans la couche Bronze
python ingest_raw.py
```

### 3. Nettoyage et transformation (Silver Layer)

```bash
# Nettoyer les données
python clean_data.py

# Traiter les données de consommation
python traitement_donnees_conso.py

# Traiter les données météo
python traitement_donnees_meteo.py

# Créer la couche Silver
python Silver.py
```

### 4. Entraînement des modèles

```bash
# Modèle de régression linéaire
python linear_regression.py

# Modèle Random Forest
python random_forest.py
```

### 5. Lancer l'application

```bash
python app.py
```

## 📊 Analyse exploratoire

Utilisez le notebook Jupyter pour explorer les données :

```bash
jupyter notebook eda_template.ipynb
```

## 📈 Modèles de Machine Learning

Le projet implémente deux approches de prédiction :

### Régression Linéaire
- Modèle simple et interprétable
- Adapté pour comprendre les relations linéaires entre variables météo et consommation

### Random Forest
- Modèle plus complexe capturant les interactions non-linéaires
- Meilleure performance prédictive sur des patterns complexes

## 🔗 Sources de données

Les sources de données utilisées sont documentées dans `api_source.txt`.

Principales sources :
- **Enedis** : Données de consommation électrique
- **Données météorologiques** : Température, précipitations, etc.

## 🤝 Contribution

Les contributions sont les bienvenues ! N'hésitez pas à :

1. Fork le projet
2. Créer une branche pour votre fonctionnalité (`git checkout -b feature/AmazingFeature`)
3. Commit vos changements (`git commit -m 'Add some AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 👥 Contributeurs

Ce projet a été développé par une équipe de 4 contributeurs dans le cadre d'un hackathon.

## 📝 Licence

Ce projet est développé dans le cadre d'un hackathon. Pour toute question de licence, veuillez contacter les contributeurs.

## 🎯 Roadmap

- [ ] Ajouter d'autres sources de données (qualité de l'air, trafic)
- [ ] Implémenter des modèles de deep learning (LSTM pour séries temporelles)
- [ ] Créer un dashboard interactif de visualisation
- [ ] Ajouter des tests unitaires
- [ ] Déploiement en production

## 📞 Contact

Pour toute question ou suggestion, n'hésitez pas à ouvrir une issue sur GitHub.

---

Développé avec ❤️ pour des villes plus durables

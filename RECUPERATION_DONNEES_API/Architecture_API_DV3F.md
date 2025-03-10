Architecture et Logique du Script de Récupération des Données DV3F
Introduction
Ce document décrit l'architecture, la logique, les bibliothèques utilisées et les fonctions du script Recuperation_donnees_API_DV3F.py. Ce script permet de récupérer des données immobilières depuis l'API DV3F (Demande de Valeurs Foncières) du Cerema et de les stocker dans une base de données MySQL.

Architecture Globale
Le script est organisé selon une architecture modulaire avec plusieurs sections distinctes :

Imports et Configuration : Chargement des bibliothèques et configuration initiale
Connexion à la Base de Données : Fonctions pour établir la connexion à MySQL
Appel à l'API : Fonctions pour interagir avec l'API DV3F
Création des Tables : Définition du schéma de la base de données
Import des Données : Fonctions pour récupérer et stocker les données
Fonction Principale : Orchestration du processus d'importation
Point d'Entrée : Démarrage du script
Cette architecture permet une séparation claire des responsabilités et facilite la maintenance du code.

Bibliothèques Utilisées
Le script utilise plusieurs bibliothèques Python pour accomplir ses tâches :

Bibliothèque	Utilisation
requests	Effectuer des requêtes HTTP vers l'API DV3F
pandas	Manipuler et transformer les données récupérées
mysql.connector	Interagir avec la base de données MySQL
os	Accéder aux variables d'environnement
dotenv	Charger les variables d'environnement depuis un fichier .env
time	Gérer les pauses entre les requêtes API
json	Manipuler les données au format JSON
tqdm	Afficher des barres de progression (non utilisé actuellement)
Description des Fonctions
1. Connexion à la Base de Données
def get_mysql_connection()
Copy
Insert
Apply
Objectif : Établir une connexion à la base de données MySQL
Paramètres : Aucun (utilise les variables d'environnement)
Retour : Objet de connexion MySQL ou None en cas d'erreur
Logique : Utilise les informations de connexion (hôte, utilisateur, mot de passe, base de données) depuis les variables d'environnement ou des valeurs par défaut
2. Appel à l'API
def apidf(url_endpoint, token=None, timeout=30)
Copy
Insert
Apply
Objectif : Effectuer une requête à l'API DV3F et récupérer les résultats
Paramètres :
url_endpoint : URL de l'endpoint à appeler
token : Token d'authentification (optionnel)
timeout : Délai d'attente maximum en secondes
Retour : Données JSON retournées par l'API ou None en cas d'erreur
Logique :
Effectue une requête GET avec les en-têtes appropriés
Gère les erreurs et les timeouts
Réessaie avec un timeout plus long en cas d'échec
3. Création des Tables
def create_tables(connection)
Copy
Insert
Apply
Objectif : Créer les tables nécessaires dans la base de données MySQL
Paramètres : connection - Connexion MySQL active
Retour : Aucun
Logique :
Crée deux tables principales :
dv3f_indicateurs_commune : Stocke les indicateurs annuels par commune
dv3f_mutations : Stocke les mutations géolocalisées
Utilise la clause IF NOT EXISTS pour éviter les erreurs si les tables existent déjà
4. Import des Données
4.1 Import des Indicateurs par Commune
def import_indicateurs_commune(connection, code_insee, nom_commune=None)
Copy
Insert
Apply
Objectif : Récupérer et sauvegarder les indicateurs annuels pour une commune
Paramètres :
connection : Connexion MySQL active
code_insee : Code INSEE de la commune
nom_commune : Nom de la commune (optionnel)
Retour : Aucun
Logique :
Appelle l'API pour récupérer les indicateurs de la commune
Convertit les résultats en DataFrame pandas
Insère les données dans la table dv3f_indicateurs_commune
Gère les doublons avec ON DUPLICATE KEY UPDATE
4.2 Import des Mutations Géolocalisées
def import_mutations_geoloc(connection, bbox=None, code_insee=None, max_retries=3)
Copy
Insert
Apply
Objectif : Récupérer et sauvegarder les mutations géolocalisées
Paramètres :
connection : Connexion MySQL active
bbox : Bounding box (x1, y1, x2, y2) pour filtrer par zone géographique
code_insee : Code INSEE pour filtrer par commune
max_retries : Nombre maximum de tentatives en cas d'échec
Retour : Aucun
Logique :
Construit l'URL appropriée selon les paramètres (bbox ou code_insee)
Appelle l'API avec pagination (traite toutes les pages de résultats)
Vérifie les IDs de mutation en double
Extrait les coordonnées géographiques et autres propriétés
Insère les données dans la table dv3f_mutations
Effectue des commits par lots pour optimiser les performances
5. Fonction Principale
def main()
Copy
Insert
Apply
Objectif : Orchestrer l'ensemble du processus d'importation
Paramètres : Aucun
Retour : Aucun
Logique :
Définit la liste des communes à traiter
Établit la connexion à la base de données
Crée les tables nécessaires
Importe les indicateurs pour chaque commune
Importe les mutations géolocalisées pour le centre de Rennes
Gère les erreurs et assure la fermeture de la connexion
Flux de Données
Récupération : Les données sont récupérées depuis l'API DV3F via des requêtes HTTP
Transformation : Les données JSON sont converties en structures Python (dictionnaires, DataFrames)
Stockage : Les données sont insérées dans les tables MySQL appropriées
Gestion des Erreurs : Les erreurs sont capturées et traitées à chaque étape
Mécanismes de Robustesse
Le script intègre plusieurs mécanismes pour assurer sa robustesse :

Gestion des Timeouts : Réessaie avec un timeout plus long en cas d'échec
Mécanisme de Reprise : Tentatives multiples pour les appels API qui échouent
Commits par Lots : Sauvegarde régulière des données pour éviter de perdre tout le travail en cas d'erreur
Détection des Doublons : Identifie les IDs de mutation en double
Clause ON DUPLICATE KEY UPDATE : Gère proprement les insertions de données existantes
Architecture des Scripts de Collecte et Chargement de Données Immobilières
1. Script "big_data.py"
Architecture générale
Le script "big_data.py" est conçu pour extraire des données géographiques et démographiques sur les villes françaises depuis Wikidata via des requêtes SPARQL. Il s'organise autour de trois fonctions principales et suit une architecture modulaire.

Bibliothèques utilisées
pandas : Utilisée pour la manipulation et le stockage structuré des données sous forme de DataFrames, facilitant l'exportation vers CSV.
SPARQLWrapper : Bibliothèque spécialisée qui permet d'exécuter des requêtes SPARQL sur des endpoints comme Wikidata, offrant une interface Python pour interagir avec des bases de connaissances sémantiques.
Fonctions principales
run_sparql_query(query)

Rôle : Exécute une requête SPARQL sur l'endpoint Wikidata
Entrée : Une chaîne de caractères contenant la requête SPARQL
Sortie : Les résultats au format JSON ou None en cas d'erreur
Fonctionnement : Configure SPARQLWrapper avec l'URL de l'endpoint Wikidata, définit le format de retour et gère les exceptions
get_city_data(city_name)

Rôle : Récupère diverses informations sur une ville spécifique
Entrée : Le nom de la ville (en français)
Sortie : Un DataFrame pandas contenant les données structurées de la ville
Fonctionnement :
Exécute plusieurs requêtes SPARQL pour obtenir différentes informations (population, superficie, code postal, etc.)
Traite les résultats pour extraire les valeurs pertinentes
Calcule des métriques dérivées comme la densité de population
Compile toutes les données dans un DataFrame
Section principale

Rôle : Point d'entrée du script qui utilise les fonctions définies
Fonctionnement :
Appelle get_city_data() avec le nom d'une ville ("Rennes")
Affiche les résultats
Sauvegarde les données dans un fichier CSV
Flux de données
Définition des requêtes SPARQL pour différents types d'informations
Exécution des requêtes via l'API Wikidata
Extraction et transformation des résultats
Consolidation dans une structure de données unifiée
Exportation vers un fichier CSV
2. Script "load_city_data.py"
Architecture générale
Le script "load_city_data.py" est responsable du chargement des données de villes depuis un fichier CSV vers une base de données MySQL. Il suit une architecture en trois couches : connexion à la base de données, création de la structure, et chargement des données.

Bibliothèques utilisées
pandas : Utilisée pour lire et manipuler les données du fichier CSV.
mysql.connector : Fournit une interface Python pour interagir avec les bases de données MySQL.
dotenv : Permet de charger des variables d'environnement depuis un fichier .env, sécurisant ainsi les informations de connexion.
os : Utilisée pour la manipulation des chemins de fichiers et l'accès aux variables d'environnement.
Fonctions principales
create_connection()

Rôle : Établit une connexion à la base de données MySQL
Sortie : Un objet de connexion ou None en cas d'échec
Fonctionnement : Utilise les variables d'environnement pour les paramètres de connexion avec des valeurs par défaut
create_cities_table(connection)

Rôle : Crée la table 'cities' si elle n'existe pas déjà
Entrée : Un objet de connexion à la base de données
Sortie : Un booléen indiquant le succès ou l'échec
Fonctionnement : Exécute une requête SQL CREATE TABLE avec la structure adaptée aux données des villes
load_city_data(file_path, connection)

Rôle : Charge les données du fichier CSV dans la table MySQL
Entrées : Le chemin du fichier CSV et un objet de connexion
Sortie : Un booléen indiquant le succès ou l'échec
Fonctionnement :
Lit le fichier CSV avec pandas
Parcourt chaque ligne du DataFrame
Prépare les données pour l'insertion
Exécute des requêtes SQL INSERT avec gestion des doublons (ON DUPLICATE KEY UPDATE)
Effectue des commits par lots pour optimiser les performances
Maintient des compteurs pour suivre la progression
Section principale

Rôle : Point d'entrée du script qui orchestre l'exécution
Fonctionnement :
Détermine le chemin du fichier CSV
Vérifie l'existence du fichier
Établit une connexion à la base de données
Crée la table si nécessaire
Charge les données
Ferme proprement la connexion
Flux de données
Lecture du fichier CSV contenant les données des villes
Connexion à la base de données MySQL
Création de la structure de table si nécessaire
Traitement ligne par ligne des données du CSV
Insertion ou mise à jour des enregistrements dans la base de données
Suivi de la progression et gestion des erreurs
Intégration des deux scripts
Ces deux scripts forment un pipeline de données complet :

"big_data.py" extrait les données depuis Wikidata et les sauvegarde dans un format intermédiaire (CSV)
"load_city_data.py" charge ces données depuis le CSV vers une base de données relationnelle
Cette architecture en deux étapes offre plusieurs avantages :

Découplage des processus d'extraction et de chargement
Possibilité de vérifier et modifier les données entre les deux étapes
Résilience en cas d'échec (les données extraites sont sauvegardées)
Flexibilité pour ajouter d'autres sources de données ou destinations
Les deux scripts intègrent des mécanismes de gestion d'erreurs et de journalisation pour assurer la robustesse du processus et faciliter le débogage.
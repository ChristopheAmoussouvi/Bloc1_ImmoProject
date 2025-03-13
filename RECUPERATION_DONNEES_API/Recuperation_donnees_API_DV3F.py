#############################################################################
# IMPORTS ET CONFIGURATION
#############################################################################

import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import time
import json
from tqdm import tqdm

# Charger les variables d'environnement
load_dotenv()

# Configuration de l'API DV3F
BASE_URL_API = "https://apidf-preprod.cerema.fr"

#############################################################################
# FONCTIONS DE CONNEXION À LA BASE DE DONNÉES
#############################################################################

def get_mysql_connection():
    """
    Établit et retourne une connexion à la base de données MySQL.
    
    Returns:
        connection: Objet de connexion MySQL ou None en cas d'erreur
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=os.getenv("MYSQL_DATABASE", "immobilier_fnaim")
        )
        print("Connexion à la base de données établie avec succès")
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

#############################################################################
# FONCTIONS D'APPEL À L'API
#############################################################################

def apidf(url_endpoint, token=None, timeout=30):
    """
    Appelle l'API DV3F et retourne les résultats.

    Args:
        url_endpoint (str): URL de l'endpoint à appeler
        token (str, optional): Token d'authentification si nécessaire
        timeout (int, optional): Délai d'attente maximum en secondes

    Returns:
        dict: Données JSON retournées par l'API ou None en cas d'erreur
    """
    HEADERS = {
        "Content-Type": "application/json",
    }
    if token:
        HEADERS["Authorization"] = "Token " + token

    try:
        response = requests.get(
            url_endpoint,
            headers=HEADERS,
            timeout=timeout  # Ajout d'un timeout explicite
        )

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur API: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.Timeout:
        print(f"Timeout lors de l'appel à l'API: {url_endpoint}")
        print("Nouvelle tentative avec un timeout plus long...")
        # Tentative avec un timeout plus long
        try:
            response = requests.get(
                url_endpoint,
                headers=HEADERS,
                timeout=timeout * 2  # Double le timeout
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erreur API après nouvelle tentative: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Échec de la nouvelle tentative: {e}")
            return None
    except Exception as e:
        print(f"Erreur lors de l'appel à l'API: {e}")
        return None

#############################################################################
# FONCTIONS DE CRÉATION DES TABLES
#############################################################################

def create_tables(connection):
    """
    Crée les tables nécessaires dans la base de données MySQL.
    
    Args:
        connection: Connexion MySQL active
    
    Raises:
        Error: En cas d'erreur lors de la création des tables
    """
    try:
        cursor = connection.cursor()
        
        print("Création de la table des indicateurs par commune...")
        # Table pour les indicateurs annuels par commune
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dv3f_indicateurs_commune (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code_insee VARCHAR(10) NOT NULL,
            nom_commune VARCHAR(100),
            annee VARCHAR(4) NOT NULL,
            nbtrans_cod111 INT COMMENT 'Nombre de ventes de maisons individuelles',
            nbtrans_cod121 INT COMMENT 'Nombre de ventes d''appartements individuels',
            prix_median_cod111 DECIMAL(12, 2) COMMENT 'Prix médian des maisons individuelles',
            prix_median_cod121 DECIMAL(12, 2) COMMENT 'Prix médian des appartements individuels',
            surface_median_cod111 DECIMAL(8, 2) COMMENT 'Surface médiane des maisons individuelles',
            surface_median_cod121 DECIMAL(8, 2) COMMENT 'Surface médiane des appartements individuels',
            prix_m2_median_cod111 DECIMAL(10, 2) COMMENT 'Prix au m² médian des maisons individuelles',
            prix_m2_median_cod121 DECIMAL(10, 2) COMMENT 'Prix au m² médian des appartements individuels',
            date_import TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_indicateur (code_insee, annee)
        )
        ''')
        
        print("Création de la table des mutations géolocalisées...")
        # Table pour les mutations géolocalisées
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dv3f_mutations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_mutation VARCHAR(50) NOT NULL,
            code_insee VARCHAR(10),
            commune VARCHAR(100),
            datemut DATE,
            libtypbien VARCHAR(100),
            valeurfonc DECIMAL(15, 2),
            sbati DECIMAL(10, 2),
            sterr DECIMAL(10, 2),
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            date_import TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_mutation (id_mutation)
        )
        ''')
        
        connection.commit()
        print("Tables créées avec succès")
        
    except Error as e:
        print(f"Erreur lors de la création des tables: {e}")
        raise e

#############################################################################
# FONCTIONS D'IMPORT DES DONNÉES
#############################################################################

def import_indicateurs_commune(connection, code_insee, nom_commune=None):
    """
    Récupère les indicateurs annuels pour une commune et les sauvegarde dans MySQL.
    
    Args:
        connection: Connexion MySQL active
        code_insee (str): Code INSEE de la commune
        nom_commune (str, optional): Nom de la commune
    """
    print(f"Récupération des indicateurs pour la commune {code_insee} ({nom_commune if nom_commune else 'Non spécifié'})")
    
    # Construction de l'URL
    url = f"{BASE_URL_API}/indicateurs/dv3f/communes/annuel/{code_insee}"
    
    # Appel à l'API
    response = apidf(url)
    
    if not response or "results" not in response:
        print(f"Aucune donnée disponible pour la commune {code_insee}")
        return
    
    cursor = None
    try:
        # Conversion en DataFrame
        indicateurs = pd.DataFrame.from_dict(response["results"])
        
        if indicateurs.empty:
            print(f"Aucun indicateur trouvé pour la commune {code_insee}")
            return
        
        # Afficher les colonnes disponibles pour le débogage
        print("Colonnes disponibles dans les données:", indicateurs.columns.tolist())
        
        # Mapping des noms de colonnes
        column_mapping = {
            'nb_ventes_maison': 'nbtrans_cod111',
            'nb_ventes_appartement': 'nbtrans_cod121',
            'prix_median_maison': 'prix_median_cod111',
            'prix_median_appartement': 'prix_median_cod121',
            'surface_median_maison': 'surface_median_cod111',
            'surface_median_appartement': 'surface_median_cod121',
            'prix_m2_median_maison': 'prix_m2_median_cod111',
            'prix_m2_median_appartement': 'prix_m2_median_cod121'
        }
        
        # Renommer les colonnes si elles existent
        for old_col, new_col in column_mapping.items():
            if old_col in indicateurs.columns:
                indicateurs[new_col] = indicateurs[old_col]
        
        # Remplir les valeurs manquantes par 0
        for col in column_mapping.values():
            if col not in indicateurs.columns:
                indicateurs[col] = 0
            else:
                indicateurs[col] = indicateurs[col].fillna(0)
        
        # Convertir les types de données
        numeric_columns = list(column_mapping.values())
        for col in numeric_columns:
            indicateurs[col] = pd.to_numeric(indicateurs[col], errors='coerce').fillna(0)
        
        # Supprimer les doublons potentiels
        indicateurs = indicateurs.drop_duplicates(subset=['annee'], keep='last')
        
        cursor = connection.cursor()
        
        # Insertion des données
        for _, row in indicateurs.iterrows():
            try:
                query = '''
                INSERT INTO dv3f_indicateurs_commune 
                (code_insee, nom_commune, annee, nbtrans_cod111, nbtrans_cod121, 
                prix_median_cod111, prix_median_cod121, surface_median_cod111, surface_median_cod121,
                prix_m2_median_cod111, prix_m2_median_cod121)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                nom_commune = VALUES(nom_commune),
                nbtrans_cod111 = VALUES(nbtrans_cod111),
                nbtrans_cod121 = VALUES(nbtrans_cod121),
                prix_median_cod111 = VALUES(prix_median_cod111),
                prix_median_cod121 = VALUES(prix_median_cod121),
                surface_median_cod111 = VALUES(surface_median_cod111),
                surface_median_cod121 = VALUES(surface_median_cod121),
                prix_m2_median_cod111 = VALUES(prix_m2_median_cod111),
                prix_m2_median_cod121 = VALUES(prix_m2_median_cod121),
                date_import = CURRENT_TIMESTAMP
                '''
                
                data = (
                    code_insee,
                    nom_commune,
                    str(row['annee']),
                    int(row['nbtrans_cod111']),
                    int(row['nbtrans_cod121']),
                    float(row['prix_median_cod111']),
                    float(row['prix_median_cod121']),
                    float(row['surface_median_cod111']),
                    float(row['surface_median_cod121']),
                    float(row['prix_m2_median_cod111']),
                    float(row['prix_m2_median_cod121'])
                )
                
                cursor.execute(query, data)
                print(f"Insertion réussie pour l'année {row['annee']}")
                
            except Exception as e:
                print(f"Erreur lors de l'insertion pour l'année {row['annee']}: {e}")
                continue
        
        connection.commit()
        print(f"Importation réussie: {len(indicateurs)} indicateurs pour la commune {code_insee}")
        
    except Exception as e:
        print(f"Erreur lors du traitement des données: {e}")
        if cursor:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def import_mutations_geoloc(connection, bbox=None, code_insee=None, max_retries=3):
    """
    Récupère et sauvegarde les mutations géolocalisées dans la base de données
    """
    # Construction de l'URL en fonction des paramètres
    if bbox:
        x1, y1, x2, y2 = bbox
        url = f"{BASE_URL_API}/dvf_opendata/geomutations/?in_bbox={x1},{y1},{x2},{y2}&page_size=1000"
        print(f"Récupération des mutations géolocalisées pour la bbox: {x1},{y1},{x2},{y2}")
    elif code_insee:
        url = f"{BASE_URL_API}/dvf_opendata/geomutations/?code_insee={code_insee}&page_size=1000"
        print(f"Récupération des mutations géolocalisées pour la commune: {code_insee}")
    else:
        print("Erreur: Vous devez spécifier soit une bbox, soit un code_insee")
        return
    
    try:
        cursor = connection.cursor()
        total_mutations = 0
        total_pages = 0
        
        while url:
            total_pages += 1
            print(f"Traitement de la page {total_pages}...")
            
            # Implémentation de retry avec backoff exponentiel
            response = None
            retries = 0
            while response is None and retries < max_retries:
                if retries > 0:
                    print(f"Tentative {retries+1}/{max_retries}...")
                    time.sleep(5 * retries)  # Backoff exponentiel
                
                try:
                    response = apidf(url, timeout=60)
                except Exception as e:
                    print(f"Erreur lors de la requête: {e}")
                    retries += 1
                    continue
                
                retries += 1
            
            if not response or "features" not in response:
                print("Aucune donnée disponible ou format de réponse inattendu")
                break
            
            # Traitement des mutations par lots
            mutations_batch = []
            for feature in response["features"]:
                if "properties" in feature and "geometry" in feature:
                    props = feature["properties"]
                    geom = feature["geometry"]
                    
                    # Extraction et validation des coordonnées
                    longitude, latitude = None, None
                    if geom["type"] == "Point" and len(geom["coordinates"]) >= 2:
                        longitude, latitude = geom["coordinates"]
                    
                    # Nettoyage et validation des données
                    mutation_data = {
                        "id_mutation": props.get("idmutation", ""),
                        "code_insee": props.get("codinsee", ""),
                        "commune": props.get("libcom", ""),
                        "date_mut": props.get("datemut"),
                        "type_bien": props.get("libtypbien", ""),
                        "valeur": float(props.get("valeurfonc", 0) or 0),
                        "surface_batie": float(props.get("sbati", 0) or 0),
                        "surface_terrain": float(props.get("sterr", 0) or 0),
                        "latitude": latitude,
                        "longitude": longitude
                    }
                    
                    # Validation des données obligatoires
                    if mutation_data["id_mutation"] and mutation_data["code_insee"]:
                        mutations_batch.append(mutation_data)
            
            # Insertion par lots
            if mutations_batch:
                query = '''
                INSERT INTO dv3f_mutations 
                (id_mutation, code_insee, commune, datemut, libtypbien, valeurfonc, 
                sbati, sterr, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                code_insee = VALUES(code_insee),
                commune = VALUES(commune),
                datemut = VALUES(datemut),
                libtypbien = VALUES(libtypbien),
                valeurfonc = VALUES(valeurfonc),
                sbati = VALUES(sbati),
                sterr = VALUES(sterr),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                date_import = CURRENT_TIMESTAMP
                '''
                
                values = [(
                    m["id_mutation"],
                    m["code_insee"],
                    m["commune"],
                    m["date_mut"],
                    m["type_bien"],
                    m["valeur"],
                    m["surface_batie"],
                    m["surface_terrain"],
                    m["latitude"],
                    m["longitude"]
                ) for m in mutations_batch]
                
                try:
                    cursor.executemany(query, values)
                    connection.commit()
                    
                    total_mutations += len(mutations_batch)
                    print(f"  {total_mutations} mutations traitées...")
                except Exception as e:
                    print(f"Erreur lors de l'insertion des données: {e}")
                    connection.rollback()
            
            # Passage à la page suivante
            url = response.get("next")
            if url:
                time.sleep(2)  # Pause entre les pages pour éviter de surcharger l'API
        
        print(f"Importation réussie: {total_mutations} mutations sur {total_pages} pages")
        
    except Exception as e:
        print(f"Erreur lors de l'insertion des mutations: {e}")
        if connection.is_connected():
            connection.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

#############################################################################
# FONCTION PRINCIPALE
#############################################################################

def main():
    """
    Fonction principale qui orchestre l'importation des données.
    """
    print("=" * 80)
    print("DÉMARRAGE DE L'IMPORTATION DES DONNÉES DV3F")
    print("=" * 80)
    
    # Liste des communes à traiter (code INSEE et nom)
    communes = [
        ("35238", "Rennes"),
        ("75056", "Paris"),
        ("69123", "Lyon"),
        ("13055", "Marseille"),
        ("33063", "Bordeaux"),
        ("59350", "Lille"),
        ("44109", "Nantes"),
        ("31555", "Toulouse"),
        ("67482", "Strasbourg"),
        ("06088", "Nice")
    ]
    
    # Établir la connexion à MySQL
    connection = get_mysql_connection()
    if not connection:
        print("Impossible de se connecter à la base de données. Arrêt du script.")
        return
    
    try:
        print("\n" + "=" * 80)
        print("CRÉATION DES TABLES")
        print("=" * 80)
        # Créer les tables si elles n'existent pas
        create_tables(connection)
        
        print("\n" + "=" * 80)
        print("IMPORTATION DES INDICATEURS PAR COMMUNE")
        print("=" * 80)
        # Importer les indicateurs pour chaque commune
        for code_insee, nom in communes:
            try:
                import_indicateurs_commune(connection, code_insee, nom)
                time.sleep(2)  # Augmenter la pause entre les requêtes pour éviter de surcharger l'API
            except Exception as e:
                print(f"Erreur lors de l'importation des indicateurs pour {nom} ({code_insee}): {e}")
                print("Passage à la commune suivante...")
                continue
        
        print("\n" + "=" * 80)
        print("IMPORTATION DES MUTATIONS GÉOLOCALISÉES")
        print("=" * 80)
        # Exemple d'import de mutations géolocalisées pour Rennes
        # Définir une bbox autour du centre de Rennes
        rennes_center = (-1.676587234535742, 48.11772222119084)  # longitude, latitude
        delta = 0.007  # environ 1km
        bbox = (
            rennes_center[0] - delta,  # x1
            rennes_center[1] - delta,  # y1
            rennes_center[0] + delta,  # x2
            rennes_center[1] + delta   # y2
        )
        
        print("\n--- Importation des mutations pour le centre de Rennes ---")
        import_mutations_geoloc(connection, bbox=bbox)
        
        print("\n" + "=" * 80)
        print("IMPORTATION DES DONNÉES TERMINÉE AVEC SUCCÈS")
        print("=" * 80)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("ERREUR LORS DE L'EXÉCUTION DU SCRIPT")
        print("=" * 80)
        print(f"Détail de l'erreur: {e}")
    finally:
        if connection:
            connection.close()
            print("\nConnexion à la base de données fermée")

#############################################################################
# POINT D'ENTRÉE DU SCRIPT
#############################################################################

if __name__ == "__main__":
    main()
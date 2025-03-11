
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import re
import json
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def create_connection():
    """Établit une connexion à la base de données MySQL."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=os.getenv("MYSQL_DATABASE", "immobilier_fnaim")
        )
        return connection
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

def create_cities_table(connection):
    """Crée la table 'cities' si elle n'existe pas déjà."""
    try:
        cursor = connection.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ville VARCHAR(100) NOT NULL,
            population INT,
            surface FLOAT,
            date VARCHAR(50),
            densite FLOAT,
            villes_voisines TEXT,
            code_postal VARCHAR(10),
            coordonnees TEXT,
            demonym VARCHAR(100),
            region_label VARCHAR(100),
            UNIQUE KEY unique_city (ville, code_postal)
        )
        ''')
        
        connection.commit()
        print("Table 'cities' créée ou déjà existante")
        return True
    except Error as e:
        print(f"Erreur lors de la création de la table: {e}")
        return False

def preprocess_data(df):
    """Prétraite les données avant insertion dans la base de données."""
    # Copie du DataFrame pour éviter de modifier l'original
    processed_df = df.copy()
    
    # Normalisation des noms de colonnes (suppression des accents, espaces, etc.)
    processed_df.columns = [col.replace('é', 'e').replace('è', 'e').replace('ê', 'e').replace(' ', '_').lower() for col in processed_df.columns]
    
    # Renommer les colonnes si nécessaire pour correspondre à la structure de la table
    column_mapping = {
        'code_postale': 'code_postal',
        'densite': 'densite'
    }
    processed_df.rename(columns=column_mapping, inplace=True)
    
    # Conversion des types de données
    if 'population' in processed_df.columns:
        processed_df['population'] = pd.to_numeric(processed_df['population'], errors='coerce')
    
    if 'surface' in processed_df.columns:
        processed_df['surface'] = pd.to_numeric(processed_df['surface'], errors='coerce')
    
    if 'densite' in processed_df.columns:
        processed_df['densite'] = pd.to_numeric(processed_df['densite'], errors='coerce')
    
    # Nettoyage des chaînes de caractères
    for col in ['ville', 'demonym', 'region_label']:
        if col in processed_df.columns:
            processed_df[col] = processed_df[col].astype(str).str.strip()
    
    # Traitement des codes postaux
    if 'code_postal' in processed_df.columns:
        # Extraction du premier code postal si plusieurs sont présents
        processed_df['code_postal'] = processed_df['code_postal'].astype(str).apply(
            lambda x: re.search(r'\d{5}', x).group(0) if re.search(r'\d{5}', x) else None
        )
    
    # Traitement des coordonnées
    if 'coordonnees' in processed_df.columns:
        # Normalisation du format JSON
        processed_df['coordonnees'] = processed_df['coordonnees'].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else 
                     (json.dumps(json.loads(x)) if isinstance(x, str) and x.strip().startswith('{') else x)
        )
    
    # Traitement des villes voisines
    if 'villes_voisines' in processed_df.columns:
        # Conversion en format JSON si ce n'est pas déjà le cas
        processed_df['villes_voisines'] = processed_df['villes_voisines'].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else 
                     (json.dumps(json.loads(x)) if isinstance(x, str) and x.strip().startswith('[') else x)
        )
    
    return processed_df

def load_city_data(file_path, connection):
    """Charge les données de city_data.csv dans la table 'cities'."""
    try:
        # Lire le fichier CSV
        df = pd.read_csv(file_path)
        
        # Prétraitement des données
        processed_df = preprocess_data(df)
        
        # Vérifier les colonnes existantes dans le DataFrame
        print(f"Colonnes dans le DataFrame prétraité: {', '.join(processed_df.columns)}")
        
        # Créer un curseur pour exécuter les requêtes SQL
        cursor = connection.cursor()
        
        # Compteurs pour le suivi
        records_processed = 0
        records_inserted = 0
        records_skipped = 0
        
        # Traitement des données
        for _, row in processed_df.iterrows():
            try:
                # Préparer les données
                data = (
                    row.get('ville', ''),
                    row.get('population', None),
                    row.get('surface', None),
                    row.get('date', None),
                    row.get('densite', None),
                    row.get('villes_voisines', None),
                    row.get('code_postal', None),
                    row.get('coordonnees', None),
                    row.get('demonym', None),
                    row.get('region_label', None)
                )
                
                # Requête d'insertion avec gestion des doublons
                query = '''
                INSERT INTO cities 
                (ville, population, surface, date, densite, villes_voisines, code_postal, coordonnees, demonym, region_label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                population = VALUES(population),
                surface = VALUES(surface),
                date = VALUES(date),
                densite = VALUES(densite),
                villes_voisines = VALUES(villes_voisines),
                coordonnees = VALUES(coordonnees),
                demonym = VALUES(demonym),
                region_label = VALUES(region_label)
                '''
                
                cursor.execute(query, data)
                
                records_processed += 1
                if cursor.rowcount > 0:
                    records_inserted += 1
                else:
                    records_skipped += 1
                
                # Commit par lots pour optimiser les performances
                if records_processed % 100 == 0:
                    connection.commit()
                    print(f"Traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
                
            except Exception as e:
                print(f"Erreur lors du traitement de la ligne {records_processed}: {e}")
                records_skipped += 1
        
        # Commit final
        connection.commit()
        print(f"Importation terminée. Total traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
        
        return True
        
    except Exception as e:
        print(f"Erreur lors du chargement des données: {e}")
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

if __name__ == "__main__":
    # Chemin vers le fichier CSV
    file_path = os.path.join(os.path.dirname(__file__), '..', 'BIG_DATA', 'city_data.csv')
    
    # Vérifier que le fichier existe
    if not os.path.exists(file_path):
        print(f"Erreur: Le fichier {file_path} n'existe pas.")
        exit(1)
    
    # Créer une connexion à la base de données
    conn = create_connection()
    if conn is None:
        exit(1)
    
    try:
        # Créer la table si elle n'existe pas
        if create_cities_table(conn):
            # Charger les données
            load_city_data(file_path, conn)
    finally:
        # Fermer la connexion
        if conn.is_connected():
            conn.close()
            print("Connexion à la base de données fermée.")
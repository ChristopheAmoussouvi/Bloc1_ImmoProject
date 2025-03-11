
import pandas as pd
import numpy as np
import mysql.connector
import pymongo
import re
import json
import os
from dotenv import load_dotenv
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()

# Configuration des connexions aux bases de données
def create_mysql_connection():
    """Établit une connexion à la base de données MySQL."""
    try:
        # D'abord se connecter sans spécifier de base de données
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "")
        )
        
        # Créer la base de données si elle n'existe pas
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS immobilier_fnaim")
        cursor.close()
        
        # Se reconnecter avec la base de données spécifiée
        connection.database = "immobilier_fnaim"
        
        return connection
    except mysql.connector.Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        return None

def create_mongodb_connection():
    """Établit une connexion à la base de données MongoDB."""
    try:
        # Utiliser une URI valide par défaut
        mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        
        # Vérifier que l'URI commence par mongodb:// ou mongodb+srv://
        if not (mongodb_uri.startswith("mongodb://") or mongodb_uri.startswith("mongodb+srv://")):
            mongodb_uri = "mongodb://localhost:27017/"
            print(f"URI MongoDB invalide, utilisation de l'URI par défaut: {mongodb_uri}")
        
        client = pymongo.MongoClient(mongodb_uri)
        db = client[os.getenv("MONGODB_DATABASE", "immobilier_fnaim")]
        
        # Vérifier la connexion
        client.admin.command('ping')
        print("Connexion à MongoDB établie avec succès")
        
        return client, db
    except pymongo.errors.ConnectionFailure as e:
        print(f"Erreur de connexion à MongoDB: {e}")
        return None, None

# Fonctions de nettoyage des données pour les annonces
def clean_price(price_str):
    """Nettoie et convertit une chaîne de prix en valeur numérique."""
    if pd.isna(price_str) or price_str == '':
        return None
    
    # Extraction des chiffres uniquement
    if isinstance(price_str, str):
        price_digits = re.sub(r'[^\d]', '', price_str)
        if price_digits:
            return int(price_digits)
    elif isinstance(price_str, (int, float)):
        return price_str
    
    return None

def clean_surface(surface_str):
    """Nettoie et convertit une chaîne de surface en valeur numérique."""
    if pd.isna(surface_str) or surface_str == '':
        return None
    
    if isinstance(surface_str, (int, float)):
        return float(surface_str)
    
    # Extraction des chiffres avec virgule ou point
    if isinstance(surface_str, str):
        match = re.search(r'(\d+[.,]?\d*)', surface_str.replace(',', '.'))
        if match:
            return float(match.group(1).replace(',', '.'))
    
    return None

def clean_rooms(rooms_str):
    """Nettoie et convertit une chaîne de nombre de pièces en valeur numérique."""
    if pd.isna(rooms_str) or rooms_str == '':
        return None
    
    if isinstance(rooms_str, (int, float)):
        return int(rooms_str)
    
    # Extraction des chiffres uniquement
    if isinstance(rooms_str, str):
        match = re.search(r'(\d+)', rooms_str)
        if match:
            return int(match.group(1))
    
    return None

def clean_postal_code(postal_code):
    """Nettoie et valide un code postal français."""
    if pd.isna(postal_code) or postal_code == '':
        return None
    
    if isinstance(postal_code, int):
        postal_code = str(postal_code)
    
    if isinstance(postal_code, str):
        # Extraction d'un code postal à 5 chiffres
        match = re.search(r'(\d{5})', postal_code)
        if match:
            return match.group(1)
    
    return None

def extract_department(postal_code):
    """Extrait le département à partir du code postal."""
    if pd.isna(postal_code) or postal_code == '':
        return None
    
    postal_code = str(postal_code)
    
    # Cas particuliers pour les DOM-TOM
    if postal_code.startswith('97'):
        return postal_code[:3]
    # Cas particulier pour la Corse
    elif postal_code.startswith('20'):
        if int(postal_code) >= 20200:
            return '2B'  # Haute-Corse
        else:
            return '2A'  # Corse-du-Sud
    # Cas général
    else:
        return postal_code[:2]

def clean_dpe_value(dpe_str):
    """Nettoie et convertit une valeur DPE en valeur numérique."""
    if pd.isna(dpe_str) or dpe_str == '':
        return None
    
    if isinstance(dpe_str, (int, float)):
        return int(dpe_str)
    
    if isinstance(dpe_str, str):
        # Extraction des chiffres uniquement
        match = re.search(r'(\d+)', dpe_str)
        if match:
            return int(match.group(1))
    
    return None

def clean_dpe_letter(dpe_str):
    """Extrait la lettre de classification DPE."""
    if pd.isna(dpe_str) or dpe_str == '':
        return None
    
    if isinstance(dpe_str, str):
        # Extraction d'une lettre de A à G
        match = re.search(r'[A-G]', dpe_str.upper())
        if match:
            return match.group(0)
    
    return None

def clean_phone(phone_str):
    """Nettoie et formate un numéro de téléphone."""
    if pd.isna(phone_str) or phone_str == '':
        return None
    
    if isinstance(phone_str, (int, float)):
        phone_str = str(int(phone_str))
    
    if isinstance(phone_str, str):
        # Extraction des chiffres uniquement
        digits = re.sub(r'[^\d]', '', phone_str)
        if len(digits) >= 10:
            return digits[:10]  # Garder les 10 premiers chiffres
    
    return None

def clean_siret(siret_str):
    """Nettoie et valide un numéro SIRET."""
    if pd.isna(siret_str) or siret_str == '':
        return None
    
    if isinstance(siret_str, (int, float)):
        siret_str = str(int(siret_str))
    
    if isinstance(siret_str, str):
        # Extraction des chiffres uniquement
        digits = re.sub(r'[^\d]', '', siret_str)
        if len(digits) == 14:  # Un SIRET valide a 14 chiffres
            return digits
    
    return None

# Prétraitement des données d'annonces
def preprocess_annonces_dataframe(df):
    """Prétraite le DataFrame d'annonces avant insertion dans les bases de données."""
    # Copie du DataFrame pour éviter de modifier l'original
    processed_df = df.copy()
    
    # Nettoyage des colonnes principales
    if 'prix' in processed_df.columns:
        processed_df['prix'] = processed_df['prix'].apply(clean_price)
    
    if 'surface' in processed_df.columns:
        processed_df['surface'] = processed_df['surface'].apply(clean_surface)
    
    if 'nb_pieces' in processed_df.columns:
        processed_df['nb_pieces'] = processed_df['nb_pieces'].apply(clean_rooms)
    
    if 'nb_chambres' in processed_df.columns:
        processed_df['nb_chambres'] = processed_df['nb_chambres'].apply(clean_rooms)
    
    if 'code_postal' in processed_df.columns:
        processed_df['code_postal'] = processed_df['code_postal'].apply(clean_postal_code)
        processed_df['departement'] = processed_df['code_postal'].apply(extract_department)
    
    # Nettoyage des données DPE/GES
    if 'dpe_consumption' in processed_df.columns:
        processed_df['dpe_consumption'] = processed_df['dpe_consumption'].apply(clean_dpe_value)
    
    if 'ges_emission' in processed_df.columns:
        processed_df['ges_emission'] = processed_df['ges_emission'].apply(clean_dpe_value)
    
    if 'dpe_rating' in processed_df.columns:
        processed_df['dpe_rating'] = processed_df['dpe_rating'].apply(clean_dpe_letter)
    
    if 'ges_rating' in processed_df.columns:
        processed_df['ges_rating'] = processed_df['ges_rating'].apply(clean_dpe_letter)
    
    # Calcul du prix au m²
    if 'prix' in processed_df.columns and 'surface' in processed_df.columns:
        mask = (processed_df['prix'].notna() & processed_df['surface'].notna() & (processed_df['surface'] > 0))
        processed_df.loc[mask, 'prix_m2'] = processed_df.loc[mask, 'prix'] / processed_df.loc[mask, 'surface']
    
    # Conversion des dates
    if 'date_publication' in processed_df.columns:
        processed_df['date_publication'] = pd.to_datetime(processed_df['date_publication'], errors='coerce')
    
    # Remplacer toutes les valeurs NaN par None pour éviter les problèmes avec MySQL
    for col in processed_df.columns:
        processed_df[col] = processed_df[col].where(pd.notna(processed_df[col]), None)
    
    # Suppression des lignes sans informations essentielles
    essential_columns = ['prix', 'surface', 'code_postal']
    processed_df = processed_df.dropna(subset=essential_columns, how='all')
    
    return processed_df

# Prétraitement des données d'agences
def preprocess_agences_dataframe(df):
    """Prétraite le DataFrame d'agences avant insertion dans les bases de données."""
    # Copie du DataFrame pour éviter de modifier l'original
    processed_df = df.copy()
    
    # Nettoyage des colonnes principales
    if 'agency_id' in processed_df.columns:
        processed_df['agency_id'] = processed_df['agency_id'].apply(lambda x: int(x) if pd.notna(x) and x != '' else None)
    
    if 'agency_phone' in processed_df.columns:
        processed_df['agency_phone'] = processed_df['agency_phone'].apply(clean_phone)
    
    if 'agency_siret' in processed_df.columns:
        processed_df['agency_siret'] = processed_df['agency_siret'].apply(clean_siret)
    
    # Conversion des dates
    if 'date_scrape' in processed_df.columns:
        processed_df['date_scrape'] = pd.to_datetime(processed_df['date_scrape'], errors='coerce')
    
    # Remplacer toutes les valeurs NaN par None pour éviter les problèmes avec MySQL
    for col in processed_df.columns:
        processed_df[col] = processed_df[col].where(pd.notna(processed_df[col]), None)
    
    # Suppression des lignes sans ID d'agence
    if 'agency_id' in processed_df.columns:
        processed_df = processed_df.dropna(subset=['agency_id'])
    
    return processed_df

# Fonctions d'insertion dans les bases de données pour les annonces
def insert_annonces_into_mysql(df, connection):
    """Insère les données d'annonces dans la base MySQL."""
    if connection is None:
        print("Pas de connexion MySQL disponible.")
        return False
    
    try:
        cursor = connection.cursor()
        
        # Création de la table avec un champ département plus grand (VARCHAR(10) au lieu de VARCHAR(3))
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS annonces (
            id INT AUTO_INCREMENT PRIMARY KEY,
            titre VARCHAR(255),
            prix DECIMAL(12, 2),
            surface DECIMAL(8, 2),
            nb_pieces INT,
            nb_chambres INT,
            type_habitation VARCHAR(50),
            code_postal VARCHAR(10),
            departement VARCHAR(10),
            ville VARCHAR(100),
            dpe_consumption INT,
            dpe_rating CHAR(1),
            ges_emission INT,
            ges_rating CHAR(1),
            prix_m2 DECIMAL(10, 2),
            date_publication DATE,
            reference VARCHAR(50),
            url VARCHAR(255),
            agency_id INT,
            UNIQUE KEY unique_annonce (reference)
        )
        ''')
        
        # Compteurs pour le suivi
        records_processed = 0
        records_inserted = 0
        records_skipped = 0
        
        # Insertion des données
        for _, row in df.iterrows():
            try:
                # Préparation des données avec gestion explicite des None
                data = []
                for field in ['titre', 'prix', 'surface', 'nb_pieces', 'nb_chambres', 'type_habitation', 
                             'code_postal', 'departement', 'ville', 'dpe_consumption', 'dpe_rating', 
                             'ges_emission', 'ges_rating', 'prix_m2', 'date_publication', 'reference', 'url', 'agency_id']:
                    value = row.get(field, None)
                    # Convertir explicitement les NaN en None
                    if pd.isna(value):
                        value = None
                    data.append(value)
                
                # Requête d'insertion
                query = '''
                INSERT INTO annonces 
                (titre, prix, surface, nb_pieces, nb_chambres, type_habitation, 
                code_postal, departement, ville, dpe_consumption, dpe_rating, 
                ges_emission, ges_rating, prix_m2, date_publication, reference, url, agency_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                titre = VALUES(titre),
                prix = VALUES(prix),
                surface = VALUES(surface),
                nb_pieces = VALUES(nb_pieces),
                nb_chambres = VALUES(nb_chambres),
                type_habitation = VALUES(type_habitation),
                code_postal = VALUES(code_postal),
                departement = VALUES(departement),
                ville = VALUES(ville),
                dpe_consumption = VALUES(dpe_consumption),
                dpe_rating = VALUES(dpe_rating),
                ges_emission = VALUES(ges_emission),
                ges_rating = VALUES(ges_rating),
                prix_m2 = VALUES(prix_m2),
                date_publication = VALUES(date_publication),
                url = VALUES(url),
                agency_id = VALUES(agency_id)
                '''
                
                cursor.execute(query, tuple(data))
                
                records_processed += 1
                if cursor.rowcount > 0:
                    records_inserted += 1
                else:
                    records_skipped += 1
                
                # Commit par lots
                if records_processed % 100 == 0:
                    connection.commit()
                    print(f"MySQL Annonces - Traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
                
            except mysql.connector.Error as e:
                print(f"Erreur MySQL lors du traitement de la ligne {records_processed}: {e}")
                records_skipped += 1
        
        # Commit final
        connection.commit()
        print(f"MySQL Annonces - Importation terminée. Total traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"Erreur MySQL: {e}")
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

def insert_agences_into_mysql(df, connection):
    """Insère les données d'agences dans la base MySQL."""
    if connection is None:
        print("Pas de connexion MySQL disponible.")
        return False
    
    try:
        cursor = connection.cursor()
        
        # Création de la table si elle n'existe pas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS agences (
            id INT PRIMARY KEY,
            nom VARCHAR(255),
            adresse TEXT,
            code_postal VARCHAR(10),
            ville VARCHAR(100),
            telephone VARCHAR(20),
            siret VARCHAR(14),
            carte_pro VARCHAR(50),
            representant_legal VARCHAR(255),
            url VARCHAR(255),
            date_scrape DATETIME
        )
        ''')
        
        # Compteurs pour le suivi
        records_processed = 0
        records_inserted = 0
        records_skipped = 0
        
        # Insertion des données
        for _, row in df.iterrows():
            try:
                # Extraction du code postal et de la ville depuis l'adresse
                adresse_complete = row.get('agency_address', '')
                code_postal = None
                ville = None
                
                if isinstance(adresse_complete, str):
                    # Recherche d'un code postal dans l'adresse
                    cp_match = re.search(r'(\d{5})\s+([A-Z\s]+)', adresse_complete.upper())
                    if cp_match:
                        code_postal = cp_match.group(1)
                        ville = cp_match.group(2).strip()
                
                # Préparation des données avec gestion explicite des None
                data = []
                for field, value in [
                    ('agency_id', row.get('agency_id', None)),
                    ('agency_name', row.get('agency_name', None)),
                    ('agency_address', adresse_complete),
                    ('code_postal', code_postal),
                    ('ville', ville),
                    ('agency_phone', row.get('agency_phone', None)),
                    ('agency_siret', row.get('agency_siret', None)),
                    ('agency_card_number', row.get('agency_card_number', None)),
                    ('agency_legal_reps', row.get('agency_legal_reps', None)),
                    ('agency_url', row.get('agency_url', None)),
                    ('date_scrape', row.get('date_scrape', None))
                ]:
                    # Convertir explicitement les NaN en None
                    if pd.isna(value):
                        value = None
                    data.append(value)
                
                # Requête d'insertion
                query = '''
                INSERT INTO agences 
                (id, nom, adresse, code_postal, ville, telephone, siret, carte_pro, representant_legal, url, date_scrape)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                nom = VALUES(nom),
                adresse = VALUES(adresse),
                code_postal = VALUES(code_postal),
                ville = VALUES(ville),
                telephone = VALUES(telephone),
                siret = VALUES(siret),
                carte_pro = VALUES(carte_pro),
                representant_legal = VALUES(representant_legal),
                url = VALUES(url),
                date_scrape = VALUES(date_scrape)
                '''
                
                cursor.execute(query, tuple(data))
                
                records_processed += 1
                if cursor.rowcount > 0:
                    records_inserted += 1
                else:
                    records_skipped += 1
                
                # Commit par lots
                if records_processed % 50 == 0:
                    connection.commit()
                    print(f"MySQL Agences - Traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
                
            except mysql.connector.Error as e:
                print(f"Erreur MySQL lors du traitement de la ligne {records_processed}: {e}")
                records_skipped += 1
        
        # Commit final
        connection.commit()
        print(f"MySQL Agences - Importation terminée. Total traités: {records_processed}, Insérés: {records_inserted}, Ignorés: {records_skipped}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"Erreur MySQL: {e}")
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

def insert_annonces_into_mongodb(df, db):
    """Insère les données d'annonces dans la base MongoDB."""
    if db is None:
        print("Pas de connexion MongoDB disponible.")
        return False
    
    try:
        # Création de la collection
        collection = db["annonces"]
        
        # Conversion du DataFrame en liste de dictionnaires
        records = df.to_dict('records')
        
        # Préparation des documents pour MongoDB
        documents = []
        for record in records:
            # Conversion des valeurs NaN en None
            document = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            # Ajout d'un timestamp de mise à jour
            document['updated_at'] = datetime.now()
            
            # Utilisation de la référence comme _id si disponible
            if 'reference' in document and document['reference']:
                document['_id'] = document['reference']
            
            documents.append(document)
        
        # Insertion des documents
        if documents:
            result = collection.bulk_write([
                pymongo.ReplaceOne(
                    {'_id': doc['_id']}, doc, upsert=True
                ) if '_id' in doc else pymongo.InsertOne(doc)
                for doc in documents
            ])
            
            print(f"MongoDB Annonces - Importation terminée. Insérés: {result.upserted_count}, Modifiés: {result.modified_count}")
        else:
            print("Aucun document à insérer dans MongoDB.")
        
        return True
        
    except pymongo.errors.PyMongoError as e:
        print(f"Erreur MongoDB: {e}")
        return False

def insert_agences_into_mongodb(df, db):
    """Insère les données d'agences dans la base MongoDB."""
    if db is None:
        print("Pas de connexion MongoDB disponible.")
        return False
    
    try:
        # Création de la collection
        collection = db["agences"]
        
        # Conversion du DataFrame en liste de dictionnaires
        records = df.to_dict('records')
        
        # Préparation des documents pour MongoDB
        documents = []
        for record in records:
            # Conversion des valeurs NaN en None
            document = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            # Ajout d'un timestamp de mise à jour
            document['updated_at'] = datetime.now()
            
            # Utilisation de l'ID d'agence comme _id si disponible
            if 'agency_id' in document and document['agency_id']:
                document['_id'] = document['agency_id']
            
            documents.append(document)
        
        # Insertion des documents
        if documents:
            result = collection.bulk_write([
                pymongo.ReplaceOne(
                    {'_id': doc['_id']}, doc, upsert=True
                ) if '_id' in doc else pymongo.InsertOne(doc)
                for doc in documents
            ])
            
            print(f"MongoDB Agences - Importation terminée. Insérés: {result.upserted_count}, Modifiés: {result.modified_count}")
        else:
            print("Aucun document à insérer dans MongoDB.")
        
        return True
        
    except pymongo.errors.PyMongoError as e:
        print(f"Erreur MongoDB: {e}")
        return False

# Fonction principale pour traiter les annonces
def process_annonces_data(file_path):
    """Traite les données d'annonces du fichier CSV et les insère dans les bases de données."""
    try:
        # Lecture du fichier CSV
        print(f"Lecture du fichier d'annonces {file_path}...")
        df = pd.read_csv(file_path)
        print(f"Fichier chargé avec succès. {len(df)} lignes trouvées.")
        
        # Prétraitement des données
        print("Prétraitement des données d'annonces...")
        processed_df = preprocess_annonces_dataframe(df)
        print(f"Prétraitement terminé. {len(processed_df)} lignes après nettoyage.")
        
        # Connexion aux bases de données
        mysql_conn = create_mysql_connection()
        mongo_client, mongo_db = create_mongodb_connection()
        
        # Insertion dans MySQL
        if mysql_conn:
            print("Insertion des annonces dans MySQL...")
            insert_annonces_into_mysql(processed_df, mysql_conn)
        
        # Insertion dans MongoDB
        if mongo_db is not None:
            print("Insertion des annonces dans MongoDB...")
            insert_annonces_into_mongodb(processed_df, mongo_db)
        
        print("Traitement des données d'annonces terminé avec succès.")
        return True
        
    except Exception as e:
        print(f"Erreur lors du traitement des données d'annonces: {e}")
        return False
    finally:
        # Fermeture des connexions
        if 'mysql_conn' in locals() and mysql_conn:
            mysql_conn.close()
            print("Connexion MySQL fermée.")
        
        if 'mongo_client' in locals() and mongo_client:
            mongo_client.close()
            print("Connexion MongoDB fermée.")

# Fonction principale pour traiter les agences
def process_agences_data(file_path):
    """Traite les données d'agences du fichier CSV et les insère dans les bases de données."""
    try:
        # Lecture du fichier CSV
        print(f"Lecture du fichier d'agences {file_path}...")
        df = pd.read_csv(file_path)
        print(f"Fichier chargé avec succès. {len(df)} lignes trouvées.")
        
        # Prétraitement des données
        print("Prétraitement des données d'agences...")
        processed_df = preprocess_agences_dataframe(df)
        print(f"Prétraitement terminé. {len(processed_df)} lignes après nettoyage.")
        
        # Connexion aux bases de données
        mysql_conn = create_mysql_connection()
        mongo_client, mongo_db = create_mongodb_connection()
        
        # Insertion dans MySQL
        if mysql_conn:
            print("Insertion des agences dans MySQL...")
            insert_agences_into_mysql(processed_df, mysql_conn)
        
        # Insertion dans MongoDB
        if mongo_db is not None:
            print("Insertion des agences dans MongoDB...")
            insert_agences_into_mongodb(processed_df, mongo_db)
        
        print("Traitement des données d'agences terminé avec succès.")
        return True
        
    except Exception as e:
        print(f"Erreur lors du traitement des données d'agences: {e}")
        return False
    finally:
        # Fermeture des connexions
        if 'mysql_conn' in locals() and mysql_conn:
            mysql_conn.close()
            print("Connexion MySQL fermée.")
        
        if 'mongo_client' in locals() and mongo_client:
            mongo_client.close()
            print("Connexion MongoDB fermée.")

if __name__ == "__main__":
    # Chemins directs vers les fichiers CSV
    annonces_file_path = r"C:\Users\Utilisateur\Documents\Simplon (Bloc 1)\Estimateur Immobilier\SCRAPPING\SCRIPT_OK\annonces_fnaim.csv"
    agences_file_path = r"C:\Users\Utilisateur\Documents\Simplon (Bloc 1)\Estimateur Immobilier\SCRAPPING\SCRIPT_OK\agences_fnaim.csv"
    
    # Vérifier que les fichiers existent
    if not os.path.exists(annonces_file_path):
        print(f"Erreur: Le fichier {annonces_file_path} n'existe pas.")
    else:
        # Traitement des données d'annonces
        process_annonces_data(annonces_file_path)
    
    if not os.path.exists(agences_file_path):
        print(f"Erreur: Le fichier {agences_file_path} n'existe pas.")
    else:
        # Traitement des données d'agences
        process_agences_data(agences_file_path)
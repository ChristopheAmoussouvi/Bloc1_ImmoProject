# Suppression de l'avertissement bcrypt
import warnings
warnings.filterwarnings("ignore", ".*bcrypt version.*")

#Importation des modules
from fastapi import FastAPI, HTTPException, Query, Depends, status, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm, SecurityScopes
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from jose import JWTError, jwt
import bcrypt
from passlib.context import CryptContext
import uvicorn

# Charger les variables d'environnement
load_dotenv()

# Constantes pour JWT
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

# Modèles d'authentification
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: List[str] = []

class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None

class UserInDB(User):
    hashed_password: str

# Modèles de données correspondant aux structures de la base de données
class Agency(BaseModel):
    id: Optional[int] = None
    nom: Optional[str] = None
    adresse: Optional[str] = None
    code_postal: Optional[str] = None
    ville: Optional[str] = None
    telephone: Optional[str] = None
    siret: Optional[str] = None
    carte_pro: Optional[str] = None
    representant_legal: Optional[str] = None
    url: Optional[str] = None
    date_scrape: Optional[datetime] = None

class Property(BaseModel):
    id: Optional[int] = None
    titre: Optional[str] = None
    prix: Optional[float] = None
    surface: Optional[float] = None
    nb_pieces: Optional[int] = None
    nb_chambres: Optional[int] = None
    type_habitation: Optional[str] = None
    code_postal: Optional[str] = None
    departement: Optional[str] = None
    ville: Optional[str] = None
    dpe_consumption: Optional[int] = None
    dpe_rating: Optional[str] = None
    ges_emission: Optional[int] = None
    ges_rating: Optional[str] = None
    prix_m2: Optional[float] = None
    date_publication: Optional[datetime] = None
    reference: str
    url: Optional[str] = None
    agency_id: Optional[int] = None

class PropertyDetail(BaseModel):
    reference: str
    description: Optional[str] = None
    url: Optional[str] = None
    images: Optional[List[str]] = None
    updated_at: Optional[datetime] = None

class PropertyComplete(Property):
    description: Optional[str] = None
    images: Optional[List[str]] = None

class City(BaseModel):
    id: Optional[int] = None
    ville: str
    population: Optional[int] = None
    surface: Optional[float] = None
    date: Optional[str] = None
    densite: Optional[float] = None
    villes_voisines: Optional[str] = None
    code_postal: str
    coordonnees: Optional[str] = None
    demonym: Optional[str] = None
    region_label: Optional[str] = None

# Modèles de données pour DV3F
class DV3FIndicateur(BaseModel):
    id: Optional[int] = None
    code_insee: str
    nom_commune: Optional[str] = None
    annee: str
    nbtrans_cod111: Optional[int] = None
    nbtrans_cod121: Optional[int] = None
    prix_median_cod111: Optional[float] = None
    prix_median_cod121: Optional[float] = None
    surface_median_cod111: Optional[float] = None
    surface_median_cod121: Optional[float] = None
    prix_m2_median_cod111: Optional[float] = None
    prix_m2_median_cod121: Optional[float] = None
    date_import: Optional[datetime] = None

class DV3FMutation(BaseModel):
    id: Optional[int] = None
    id_mutation: str
    code_insee: Optional[str] = None
    commune: Optional[str] = None
    datemut: Optional[date] = None
    libtypbien: Optional[str] = None
    valeurfonc: Optional[float] = None
    sbati: Optional[float] = None
    sterr: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    date_import: Optional[datetime] = None

# Contexte de mot de passe
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token",
    scopes={
        "me": "Lire les informations de l'utilisateur actuel.",
        "items": "Lire les éléments.",
        "admin": "Accès administrateur."
    },
)

app = FastAPI(
    title="API Immobilier FNAIM",
    description="API pour accéder aux données immobilières structurées (MySQL) et non structurées (MongoDB)",
    version="1.0.0"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fonctions utilitaires d'authentification
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

# Exemple de base de données d'utilisateurs
fake_users_db = {
    "admin": {
        "username": "admin",
        "email": "admin@example.com",
        "full_name": "Admin User",
        "disabled": False,
        "hashed_password": get_password_hash("admin123")
    }
}

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)
    return None

def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Impossible de valider les informations d'identification",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(fake_users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Utilisateur inactif")
    return current_user

# Point de terminaison d'authentification
@app.post("/token", response_model=Token, tags=["Authentification"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me", response_model=User, tags=["Authentification"])
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

# Connexions aux bases de données
def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("DATABASE_NAME"),
            port=int(os.getenv("MYSQL_PORT", "3306"))
        )
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à MySQL: {e}")

def get_mongodb_connection():
    try:
        # Nettoyer l'URI MongoDB (supprimer "uri =" si présent)
        mongodb_uri = os.getenv("MONGODB_URI")
        if mongodb_uri and mongodb_uri.startswith("uri ="):
            mongodb_uri = mongodb_uri[5:].strip()
        
        client = MongoClient(mongodb_uri)
        return client[os.getenv("MONGODB_DATABASE")]  
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à MongoDB: {e}")

# Personnalisation du schéma OpenAPI pour inclure les sécurités
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Assurez-vous que les schémas de sécurité sont correctement définis
    if "components" not in openapi_schema:
        openapi_schema["components"] = {}
    
    if "securitySchemes" not in openapi_schema["components"]:
        openapi_schema["components"]["securitySchemes"] = {}
    
    openapi_schema["components"]["securitySchemes"]["OAuth2PasswordBearer"] = {
        "type": "oauth2",
        "flows": {
            "password": {
                "tokenUrl": "token",
                "scopes": {
                    "me": "Lire les informations de l'utilisateur actuel.",
                    "items": "Lire les éléments.",
                    "admin": "Accès administrateur."
                }
            }
        }
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Routes pour les propriétés
@app.get("/properties", response_model=List[Property], tags=["Propriétés"])
def get_properties(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    code_postal: Optional[str] = None,
    prix_min: Optional[float] = None,
    prix_max: Optional[float] = None,
    surface_min: Optional[float] = None,
    type_habitation: Optional[str] = None,
    nb_pieces_min: Optional[int] = None,
    dpe_max: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM annonces WHERE 1=1"  # Table renommée: "annonces" au lieu de "properties"
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        if prix_min:
            query += " AND prix >= %s"
            params.append(prix_min)
        
        if prix_max:
            query += " AND prix <= %s"
            params.append(prix_max)
        
        if surface_min:
            query += " AND surface >= %s"
            params.append(surface_min)
        
        if type_habitation:
            query += " AND type_habitation = %s"
            params.append(type_habitation)
        
        if nb_pieces_min:
            query += " AND nb_pieces >= %s"
            params.append(nb_pieces_min)
        
        if dpe_max:
            query += " AND dpe_rating <= %s"
            params.append(dpe_max)
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        properties = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return properties
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des propriétés: {e}")

@app.get("/properties/{reference}", response_model=PropertyComplete, tags=["Propriétés"])
def get_property(
    reference: str, 
    mysql_conn = Depends(get_mysql_connection),
    mongo_db = Depends(get_mongodb_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        # Récupérer les données structurées depuis MySQL
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM annonces WHERE reference = %s", (reference,))  # Table renommée
        property_data = cursor.fetchone()
        
        cursor.close()
        mysql_conn.close()
        
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Propriété avec référence {reference} non trouvée")
        
        # Récupérer les données non structurées depuis MongoDB
        property_details = mongo_db.annonces.find_one({"reference": reference})  # Collection renommée
        
        # Fusionner les données
        complete_property = dict(property_data)
        
        if property_details:
            if "description" in property_details:
                complete_property["description"] = property_details["description"]
            if "images" in property_details:
                complete_property["images"] = property_details["images"]
        else:
            complete_property["description"] = None
            complete_property["images"] = []
        
        return complete_property
    except Error as e:
        mysql_conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur MySQL: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {e}")

# Routes pour les agences
@app.get("/agencies", response_model=List[Agency], tags=["Agences"])
def get_agencies(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    name: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM agences"  # Table renommée: "agences"
        params = []
        
        if name:
            query += " WHERE nom LIKE %s"  # Champ renommé: "nom" au lieu de "agency_name"
            params.append(f"%{name}%")
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        agencies = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return agencies
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des agences: {e}")

@app.get("/agencies/{agency_id}", response_model=Agency, tags=["Agences"])
def get_agency(
    agency_id: int,  # ID de l'agence comme entier
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM agences WHERE id = %s", (agency_id,))  # Table et champ renommés
        agency = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not agency:
            raise HTTPException(status_code=404, detail=f"Agence avec ID {agency_id} non trouvée")
        
        return agency
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération de l'agence: {e}")

# Routes pour les villes
@app.get("/cities", response_model=List[City], tags=["Villes"])
def get_cities(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    code_postal: Optional[str] = None,
    nom_ville: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM cities WHERE 1=1"
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        if nom_ville:
            query += " AND ville LIKE %s"  # Champ renommé: "ville" au lieu de "nom_ville"
            params.append(f"%{nom_ville}%")
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        cities = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return cities
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des villes: {e}")

@app.get("/cities/{code_postal}", response_model=City, tags=["Villes"])
def get_city_by_code_postal(
    code_postal: str,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM cities WHERE code_postal = %s LIMIT 1", (code_postal,))
        city = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not city:
            raise HTTPException(status_code=404, detail=f"Ville avec code postal {code_postal} non trouvée")
        
        return city
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération de la ville: {e}")

# Routes pour les statistiques
@app.get("/stats/prix-moyen", tags=["Statistiques"])
def get_prix_moyen(
    code_postal: Optional[str] = None,
    type_habitation: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            AVG(prix) AS prix_moyen,
            COUNT(*) AS nombre_proprietes
        FROM 
            annonces  /* Table renommée: "annonces" */
        WHERE 
            prix IS NOT NULL
        """
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        if type_habitation:
            query += " AND type_habitation = %s"
            params.append(type_habitation)
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result["prix_moyen"] is None:
            return {"prix_moyen": 0, "nombre_proprietes": 0}
        
        return {
            "prix_moyen": float(result["prix_moyen"]),
            "nombre_proprietes": result["nombre_proprietes"]
        }
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul du prix moyen: {e}")

@app.get("/stats/distribution-prix", tags=["Statistiques"])
def get_distribution_prix(
    code_postal: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            CASE
                WHEN prix < 100000 THEN 'Moins de 100K€'
                WHEN prix BETWEEN 100000 AND 200000 THEN '100K€ - 200K€'
                WHEN prix BETWEEN 200001 AND 300000 THEN '200K€ - 300K€'
                WHEN prix BETWEEN 300001 AND 400000 THEN '300K€ - 400K€'
                WHEN prix BETWEEN 400001 AND 500000 THEN '400K€ - 500K€'
                ELSE 'Plus de 500K€'
            END AS tranche_prix,
            COUNT(*) AS nombre
        FROM 
            annonces  /* Table renommée: "annonces" */
        WHERE 
            prix IS NOT NULL
        """
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        query += " GROUP BY tranche_prix ORDER BY FIELD(tranche_prix, 'Moins de 100K€', '100K€ - 200K€', '200K€ - 300K€', '300K€ - 400K€', '400K€ - 500K€', 'Plus de 500K€')"
        
        cursor.execute(query, params)
        result = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return result
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors du calcul de la distribution des prix: {e}")

# Routes pour les données DV3F
@app.get("/dv3f/indicateurs", response_model=List[DV3FIndicateur], tags=["DV3F"])
def get_dv3f_indicateurs(
    code_insee: Optional[str] = None,
    annee: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """Récupère les indicateurs DV3F par commune"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM dv3f_indicateurs_commune WHERE 1=1"
        params = []
        
        if code_insee:
            query += " AND code_insee = %s"
            params.append(code_insee)
        
        if annee:
            query += " AND annee = %s"
            params.append(annee)
        
        cursor.execute(query, params)
        indicateurs = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return indicateurs
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des indicateurs DV3F: {e}")

@app.get("/dv3f/mutations", response_model=List[DV3FMutation], tags=["DV3F"])
def get_dv3f_mutations(
    code_insee: Optional[str] = None,
    commune: Optional[str] = None,
    date_min: Optional[str] = None,
    date_max: Optional[str] = None,
    type_bien: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """Récupère les mutations DV3F avec filtres"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM dv3f_mutations WHERE 1=1"
        params = []
        
        if code_insee:
            query += " AND code_insee = %s"
            params.append(code_insee)
        
        if commune:
            query += " AND commune LIKE %s"
            params.append(f"%{commune}%")
        
        if date_min:
            query += " AND datemut >= %s"
            params.append(date_min)
        
        if date_max:
            query += " AND datemut <= %s"
            params.append(date_max)
        
        if type_bien:
            query += " AND libtypbien LIKE %s"
            params.append(f"%{type_bien}%")
        
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        mutations = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return mutations
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des mutations DV3F: {e}")

@app.get("/dv3f/stats/evolution-prix", tags=["DV3F"])
def get_dv3f_evolution_prix(
    code_insee: str,
    type_bien: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """Récupère l'évolution des prix médians par année pour une commune"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            annee,
            CASE 
                WHEN %s = 'Maison' THEN prix_median_cod111
                WHEN %s = 'Appartement' THEN prix_median_cod121
                ELSE COALESCE(prix_median_cod111, prix_median_cod121)
            END as prix_median,
            CASE 
                WHEN %s = 'Maison' THEN prix_m2_median_cod111
                WHEN %s = 'Appartement' THEN prix_m2_median_cod121
                ELSE COALESCE(prix_m2_median_cod111, prix_m2_median_cod121)
            END as prix_m2_median
        FROM dv3f_indicateurs_commune
        WHERE code_insee = %s
        ORDER BY annee
        """
        
        cursor.execute(query, (type_bien, type_bien, type_bien, type_bien, code_insee))
        evolution = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return evolution
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération de l'évolution des prix: {e}")

@app.get("/dv3f/stats/comparaison-communes", tags=["DV3F"])
def get_dv3f_comparaison_communes(
    codes_insee: List[str] = Query(...),
    annee: Optional[str] = None,
    type_bien: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """Compare les indicateurs entre différentes communes"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Si l'année n'est pas spécifiée, on prend la plus récente
        if not annee:
            cursor.execute("SELECT MAX(annee) as derniere_annee FROM dv3f_indicateurs_commune")
            result = cursor.fetchone()
            annee = result['derniere_annee'] if result else None
        
        query = """
        SELECT 
            code_insee,
            nom_commune,
            annee,
            CASE 
                WHEN %s = 'Maison' THEN prix_median_cod111
                WHEN %s = 'Appartement' THEN prix_median_cod121
                ELSE COALESCE(prix_median_cod111, prix_median_cod121)
            END as prix_median,
            CASE 
                WHEN %s = 'Maison' THEN prix_m2_median_cod111
                WHEN %s = 'Appartement' THEN prix_m2_median_cod121
                ELSE COALESCE(prix_m2_median_cod111, prix_m2_median_cod121)
            END as prix_m2_median,
            CASE 
                WHEN %s = 'Maison' THEN nbtrans_cod111
                WHEN %s = 'Appartement' THEN nbtrans_cod121
                ELSE COALESCE(nbtrans_cod111, nbtrans_cod121)
            END as nombre_transactions
        FROM dv3f_indicateurs_commune
        WHERE code_insee IN ({})
        AND annee = %s
        """.format(','.join(['%s'] * len(codes_insee)))
        
        params = [type_bien] * 6 + codes_insee + [annee]
        cursor.execute(query, params)
        comparaison = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return comparaison
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la comparaison des communes: {e}")

# Route pour vérifier l'état de santé de l'API
@app.get("/health", tags=["Santé"])
def check_health():
    health_status = {"status": "OK", "connexions_bases_de_donnees": {}}
    
    # Vérifier MySQL
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        health_status["connexions_bases_de_donnees"]["mysql"] = "OK"
    except Exception as e:
        health_status["status"] = "Dégradé"
        health_status["connexions_bases_de_donnees"]["mysql"] = f"Erreur: {str(e)}"
    
    # Vérifier MongoDB
    try:
        db = get_mongodb_connection()
        db.command("ping")
        health_status["connexions_bases_de_donnees"]["mongodb"] = "OK"
    except Exception as e:
        health_status["status"] = "Dégradé"
        health_status["connexions_bases_de_donnees"]["mongodb"] = f"Erreur: {str(e)}"
    
    return health_status

# Route principale
@app.get("/", tags=["Accueil"])
def read_root():
    return {
        "message": "Bienvenue sur l'API Immobilier FNAIM",
        "version": "3.0",
        "documentation": "/docs",
        "état": "actif"
    }

# Lancement de l'application
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
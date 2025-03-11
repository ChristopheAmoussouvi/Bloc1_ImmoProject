#Importation des modules
from fastapi import FastAPI, HTTPException, Query, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List, Optional
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext

# Charger les variables d'environnement
load_dotenv()

# Constantes pour JWT
SECRET_KEY = os.getenv("SECRET_KEY") 
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

# Contexte de mot de passe
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

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

# Modèles d'authentification
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None

class UserInDB(User):
    hashed_password: str

# Fonctions utilitaires d'authentification
def verify_password(plain_password, hashed_password):
    """Vérifie si le mot de passe en clair correspond au mot de passe haché."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    """Génère un hachage sécurisé du mot de passe."""
    return pwd_context.hash(password)

def get_user(db, username: str):
    """
    Récupère un utilisateur par son nom d'utilisateur.
    
    Pour l'exemple, utilise un utilisateur codé en dur.
    À remplacer par une recherche dans la base de données.
    """
    if username == "admin":
        return UserInDB(
            username="admin",
            email="admin@example.com",
            full_name="Admin User",
            disabled=False,
            hashed_password=get_password_hash("admin123")
        )
    return None

def authenticate_user(db, username: str, password: str):
    """
    Authentifie un utilisateur avec son nom d'utilisateur et son mot de passe.
    Retourne l'utilisateur si l'authentification réussit, sinon False.
    """
    user = get_user(db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Crée un jeton d'accès JWT avec les données spécifiées et la durée d'expiration.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Récupère l'utilisateur courant à partir du jeton JWT.
    Utilisé comme dépendance pour protéger les routes.
    """
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
    user = get_user(None, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    """
    Vérifie que l'utilisateur actuel est actif (non désactivé).
    Utilisé comme dépendance pour les routes nécessitant un utilisateur actif.
    """
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Utilisateur inactif")
    return current_user

# Point de terminaison d'authentification
@app.post("/token", response_model=Token, tags=["Authentification"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Obtient un jeton d'accès pour l'authentification.
    """
    user = authenticate_user(None, form_data.username, form_data.password)
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
    """
    Récupère les informations de l'utilisateur authentifié.
    """
    return current_user

# Modèles de données
class Agency(BaseModel):
    id: Optional[int] = None
    agency_id: str
    agency_name: str
    agency_address: Optional[str] = None
    agency_url: Optional[str] = None
    agency_phone: Optional[str] = None
    agency_siret: Optional[str] = None
    agency_card_number: Optional[str] = None
    agency_legal_reps: Optional[str] = None

class Property(BaseModel):
    id: Optional[int] = None
    reference: str
    titre: Optional[str] = None
    prix: Optional[str] = None
    code_postal: Optional[str] = None
    surface: Optional[int] = None
    nb_pieces: Optional[int] = None
    nb_chambres: Optional[int] = None
    type_habitation: Optional[str] = None
    dpe_rating: Optional[str] = None
    dpe_consumption: Optional[int] = None
    ges_rating: Optional[str] = None
    ges_emission: Optional[int] = None
    parking: Optional[bool] = None
    depenses_energie_min: Optional[int] = None
    depenses_energie_max: Optional[int] = None
    date_ref_prix_energie: Optional[str] = None
    agency_id: Optional[str] = None

class PropertyDetail(BaseModel):
    reference: str
    description: Optional[str] = None
    url: Optional[str] = None
    images: Optional[List[str]] = None

class PropertyComplete(Property):
    description: Optional[str] = None
    url: Optional[str] = None
    images: Optional[List[str]] = None

class City(BaseModel):
    id: Optional[int] = None
    code_postal: str
    nom_ville: str
    departement: Optional[str] = None
    region: Optional[str] = None
    population: Optional[int] = None
    superficie: Optional[float] = None
    densite: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

@app.get("/cities", response_model=List[City], tags=["Villes"])
def get_cities(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    code_postal: Optional[str] = None,
    nom_ville: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """
    Récupère la liste des villes avec filtres.
    
    - **limit**: Nombre maximum de villes à retourner
    - **offset**: Position de départ
    - **code_postal**: Filtre par code postal (recherche exacte)
    - **nom_ville**: Filtre par nom de ville (recherche partielle)
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM cities WHERE 1=1"
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        if nom_ville:
            query += " AND nom_ville LIKE %s"
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
    """
    Récupère les détails d'une ville par son code postal.
    
    - **code_postal**: Code postal de la ville
    """
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

# Connexions aux bases de données
def get_mysql_connection():
    """Établit et retourne une connexion à la base de données MySQL."""
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
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à la base de données MySQL: {e}")

def get_mongodb_connection():
    """Établit et retourne une connexion à la base de données MongoDB."""
    try:
        client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
        db = client[os.getenv("MONGODB_DATABASE", "immobilier_fnaim")]
        return db
    except Exception as e:
        print(f"Erreur de connexion à MongoDB: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à MongoDB: {e}")

# Routes pour les agences
@app.get("/agencies", response_model=List[Agency], tags=["Agences"])
def get_agencies(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    name: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """
    Récupère la liste des agences immobilières.
    
    - **limit**: Nombre maximum d'agences à retourner
    - **offset**: Position de départ
    - **name**: Filtre par nom d'agence (recherche partielle)
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM agencies"
        params = []
        
        if name:
            query += " WHERE agency_name LIKE %s"
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
    agency_id: str, 
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """
    Récupère les détails d'une agence immobilière par son ID.
    
    - **agency_id**: Identifiant unique de l'agence
    """
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM agencies WHERE agency_id = %s", (agency_id,))
        agency = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not agency:
            raise HTTPException(status_code=404, detail=f"Agence avec ID {agency_id} non trouvée")
        
        return agency
    except Error as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération de l'agence: {e}")

# Routes pour les propriétés
@app.get("/properties", response_model=List[Property], tags=["Propriétés"])
def get_properties(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    code_postal: Optional[str] = None,
    prix_min: Optional[int] = None,
    prix_max: Optional[int] = None,
    surface_min: Optional[int] = None,
    type_habitation: Optional[str] = None,
    nb_pieces_min: Optional[int] = None,
    dpe_max: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """
    Récupère la liste des propriétés avec filtres.
    
    - **limit**: Nombre maximum de propriétés à retourner
    - **offset**: Position de départ
    - **code_postal**: Filtre par code postal
    - **prix_min**: Prix minimum
    - **prix_max**: Prix maximum
    - **surface_min**: Surface minimum en m²
    - **type_habitation**: Type d'habitation (Appartement, Maison, etc.)
    - **nb_pieces_min**: Nombre minimum de pièces
    - **dpe_max**: Performance énergétique maximum (A à G)
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM properties WHERE 1=1"
        params = []
        
        if code_postal:
            query += " AND code_postal = %s"
            params.append(code_postal)
        
        if prix_min:
            query += " AND CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) >= %s"
            params.append(prix_min)
        
        if prix_max:
            query += " AND CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) <= %s"
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
    """
    Récupère les détails complets d'une propriété par sa référence.
    
    - **reference**: Référence unique de la propriété
    """
    try:
        # Récupérer les données structurées depuis MySQL
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM properties WHERE reference = %s", (reference,))
        property_data = cursor.fetchone()
        
        cursor.close()
        mysql_conn.close()
        
        if not property_data:
            raise HTTPException(status_code=404, detail=f"Propriété avec référence {reference} non trouvée")
        
        # Récupérer les données non structurées depuis MongoDB
        property_details = mongo_db.property_details.find_one({"reference": reference})
        
        # Fusionner les données
        complete_property = dict(property_data)
        
        if property_details:
            if "description" in property_details:
                complete_property["description"] = property_details["description"]
            if "url" in property_details:
                complete_property["url"] = property_details["url"]
            if "images" in property_details:
                complete_property["images"] = property_details["images"]
        
        return complete_property
    except Error as e:
        mysql_conn.close()
        raise HTTPException(status_code=500, detail=f"Erreur MySQL: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {e}")

# Routes pour les statistiques
@app.get("/stats/prix-moyen", tags=["Statistiques"])
def get_prix_moyen(
    code_postal: Optional[str] = None,
    type_habitation: Optional[str] = None,
    conn = Depends(get_mysql_connection),
    current_user: User = Depends(get_current_active_user)
):
    """
    Calcule le prix moyen des propriétés selon les filtres.
    
    - **code_postal**: Filtre par code postal
    - **type_habitation**: Type d'habitation (Appartement, Maison, etc.)
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            AVG(CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL)) AS prix_moyen,
            COUNT(*) AS nombre_proprietes
        FROM 
            properties
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
    """
    Calcule la distribution des prix par tranches.
    
    - **code_postal**: Filtre par code postal
    """
    try:
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            CASE
                WHEN CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) < 10000 THEN 'Moins de 100K€'
                WHEN CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) BETWEEN 10000 AND 20000 THEN '100K€ - 200K€'
                WHEN CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) BETWEEN 200001 AND 30000 THEN '200K€ - 300K€'
                WHEN CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) BETWEEN 300001 AND 40000 THEN '300K€ - 400K€'
                WHEN CAST(REPLACE(REPLACE(prix, '€', ''), ' ', '') AS DECIMAL) BETWEEN 400001 AND 50000 THEN '400K€ - 500K€'
                ELSE 'Plus de 500K€'
            END AS tranche_prix,
            COUNT(*) AS nombre
        FROM 
            properties
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

# Route santé
@app.get("/health", tags=["Santé"])
def check_health():
    """Vérifie l'état de l'API et des connexions aux bases de données"""
    health_status = {"status": "OK", "database_connections": {}}
    
    # Vérifier MySQL
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        health_status["database_connections"]["mysql"] = "OK"
    except Exception as e:
        health_status["status"] = "Dégradé"
        health_status["database_connections"]["mysql"] = f"Erreur: {str(e)}"
    
    # Vérifier MongoDB
    try:
        db = get_mongodb_connection()
        db.command("ping")
        health_status["database_connections"]["mongodb"] = "OK"
    except Exception as e:
        health_status["status"] = "Dégradé"
        health_status["database_connections"]["mongodb"] = f"Erreur: {str(e)}"
    
    return health_status

# Lancement de l'application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
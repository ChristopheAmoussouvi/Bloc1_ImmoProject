import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime, date

# Initialisation de l'application Dash avec un thème Bootstrap
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True  # Ajout de cette ligne pour supprimer les exceptions de callback
)
server = app.server
app.title = "Estimateur Immobilier FNAIM"

# URL de base de l'API
API_BASE_URL = "http://localhost:8000"

# Fonction pour obtenir un token JWT
def get_jwt_token(username="admin", password="admin123"):
    """Obtient un token JWT pour l'authentification."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/token",
            data={"username": username, "password": password}
        )
        if response.status_code == 200:
            return response.json()["access_token"]
        else:
            print(f"Erreur d'authentification: {response.status_code}")
            print(response.text)
            return None
    except Exception as e:
        print(f"Erreur lors de la récupération du token: {e}")
        return None

# Fonction pour faire des requêtes API avec authentification
def api_request(endpoint, params=None, method="GET"):
    """Effectue une requête API avec token JWT."""
    token = get_jwt_token()
    if not token:
        return None
    
    headers = {"Authorization": f"Bearer {token}"}
    try:
        if method == "GET":
            response = requests.get(
                f"{API_BASE_URL}{endpoint}",
                params=params,
                headers=headers
            )
        else:
            response = requests.post(
                f"{API_BASE_URL}{endpoint}",
                json=params,
                headers=headers
            )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur API: {response.status_code}")
            print(response.text)
            return None
    except Exception as e:
        print(f"Erreur lors de la requête API: {e}")
        return None

# Fonction pour récupérer les données DV3F
def get_dv3f_data(code_insee=None, type_bien=None):
    """Récupère les données DV3F pour une commune."""
    params = {}
    if code_insee:
        params["code_insee"] = code_insee
    if type_bien:
        params["type_bien"] = type_bien
    
    return api_request("/dv3f/indicateurs", params)

def get_dv3f_evolution(code_insee, type_bien=None):
    """Récupère l'évolution des prix DV3F."""
    params = {"code_insee": code_insee}
    if type_bien:
        params["type_bien"] = type_bien
    
    return api_request("/dv3f/stats/evolution-prix", params)

def get_dv3f_mutations(code_insee=None, commune=None, date_min=None, date_max=None):
    """Récupère les mutations DV3F."""
    params = {}
    if code_insee:
        params["code_insee"] = code_insee
    if commune:
        params["commune"] = commune
    if date_min:
        params["date_min"] = date_min
    if date_max:
        params["date_max"] = date_max
    
    return api_request("/dv3f/mutations", params)

# Fonction pour créer la carte des mutations
def create_map_mutations(mutations_data):
    """Crée une carte des mutations immobilières."""
    if not mutations_data:
        return go.Figure()
    
    df = pd.DataFrame(mutations_data)
    
    fig = go.Figure(go.Scattermapbox(
        lat=df["latitude"],
        lon=df["longitude"],
        mode="markers",
        marker=go.scattermapbox.Marker(
            size=10,
            color=df["valeurfonc"],
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="Prix (€)")
        ),
        text=df.apply(lambda x: f"Prix: {x['valeurfonc']:,.0f}€<br>Surface: {x['sbati']}m²<br>Type: {x['libtypbien']}", axis=1),
        hoverinfo="text"
    ))
    
    # Centre la carte sur la moyenne des coordonnées
    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()
    
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=13
        ),
        margin=dict(r=0, t=0, l=0, b=0)
    )
    
    return fig

# Fonction pour récupérer les données depuis l'API
def get_properties(filters=None):
    """Récupère les propriétés depuis l'API avec filtres optionnels"""
    params = {}
    if filters:
        params.update(filters)
    
    try:
        response = requests.get(f"{API_BASE_URL}/properties", params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur lors de la récupération des propriétés: {response.status_code}")
            return []
    except requests.RequestException as e:
        print(f"Erreur de connexion à l'API: {e}")
        return []

def get_property_stats():
    """Récupère les statistiques des propriétés depuis l'API"""
    try:
        response = requests.get(f"{API_BASE_URL}/stats/prix-moyen")
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur lors de la récupération des stats: {response.status_code}")
            return {"prix_moyen": 0, "nombre_proprietes": 0}
    except requests.RequestException as e:
        print(f"Erreur de connexion à l'API: {e}")
        return {"prix_moyen": 0, "nombre_proprietes": 0}

def get_price_distribution(code_postal=None):
    """Récupère la distribution des prix depuis l'API"""
    params = {}
    if code_postal:
        params["code_postal"] = code_postal
    
    try:
        response = requests.get(f"{API_BASE_URL}/stats/distribution-prix", params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur lors de la récupération de la distribution: {response.status_code}")
            return []
    except requests.RequestException as e:
        print(f"Erreur de connexion à l'API: {e}")
        return []

# Fonction factice pour la prédiction de prix (à remplacer par un modèle ML plus tard)
def predict_price(features):
    """Simule une prédiction de prix basée sur les caractéristiques d'entrée"""
    # Modèle très simplifié pour la démonstration
    base_price = 200000
    
    # Ajoute l'influence de différentes caractéristiques
    if features.get('code_postal'):
        # Différents codes postaux ont des prix moyens différents
        postal_code_factor = sum([ord(char) for char in features['code_postal']]) / 500
        base_price *= postal_code_factor
    
    if features.get('surface'):
        # Les grandes propriétés coûtent plus cher
        base_price += features['surface'] * 2000
    
    if features.get('nb_pieces'):
        # Plus de pièces = plus cher
        base_price += features['nb_pieces'] * 15000
    
    if features.get('type_habitation') == 'Maison':
        # Les maisons sont généralement plus chères que les appartements
        base_price *= 1.2
    
    # Ajout d'un peu d'aléatoire pour simuler les variations du marché
    base_price *= np.random.normal(1, 0.1)
    
    return round(base_price, -3)  # Arrondi au millier près

# Fonction pour générer des données pour le graphique d'évolution des prix
def generate_price_evolution_data():
    # Utilisons des dates complètes avec année pour éviter l'avertissement
    dates = [
        date(2020, 1, 1), date(2020, 6, 1), 
        date(2021, 1, 1), date(2021, 6, 1),
        date(2022, 1, 1), date(2022, 6, 1), 
        date(2023, 1, 1), date(2023, 6, 1)
    ]
    prices = [3500, 3550, 3600, 3700, 3800, 3900, 4000, 4100]
    
    return pd.DataFrame({
        'date': dates,
        'price': prices
    })

# Création du layout de l'application
app.layout = dbc.Container([
    # En-tête
    dbc.Row([
        dbc.Col([
            html.H1("FNAIM - Estimateur Immobilier", className="text-center my-4"),
            html.P("Analysez le marché immobilier et estimez le prix des biens", className="text-center text-muted mb-4")
        ])
    ]),
    
    # Onglets de navigation
    dbc.Row([
        dbc.Col([
            dbc.Tabs([
                dbc.Tab(label="Tableau de bord", tab_id="dashboard"),
                dbc.Tab(label="Recherche de biens", tab_id="search"),
                dbc.Tab(label="Analyse du marché", tab_id="analysis"),
                dbc.Tab(label="Données DV3F", tab_id="dv3f"),
                dbc.Tab(label="Estimation de prix", tab_id="prediction"),
            ], id="tabs", active_tab="dashboard")
        ])
    ]),
    
    # Conteneur de contenu
    html.Div(id="tab-content", className="mt-4"),
    
    # Composants cachés pour les résultats
    html.Div([
        html.Div(id="search-results"),
        html.Div(id="prediction-loading"),
        html.Div(id="prediction-results"),
        html.Div(id="dv3f-evolution-graph"),
        html.Div(id="dv3f-mutations-map"),
        html.Div(id="dv3f-stats"),
    ], style={'display': 'none'}),  # Ces composants sont cachés mais disponibles pour les callbacks
    
], fluid=True)

# Callback pour la navigation entre les onglets
@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab")
)
def render_tab_content(active_tab):
    """Affiche le contenu en fonction de l'onglet actif"""
    if active_tab == "dashboard":
        return render_dashboard()
    elif active_tab == "search":
        return render_search()
    elif active_tab == "analysis":
        return render_analysis()
    elif active_tab == "dv3f":
        return render_dv3f()  # Nouvelle fonction séparée pour l'onglet DV3F
    elif active_tab == "prediction":
        return render_prediction()
    
    return html.P("Cela ne devrait pas arriver...")

def render_dashboard():
    """Affiche l'onglet tableau de bord"""
    # Récupération des statistiques pour le tableau de bord
    stats = get_property_stats()
    price_dist = get_price_distribution()
    
    # Conversion de la distribution de prix en DataFrame pour le tracé
    if price_dist:
        df_dist = pd.DataFrame(price_dist)
    else:
        # Données de démonstration si l'API ne répond pas
        df_dist = pd.DataFrame({
            "tranche_prix": ["Moins de 100K€", "100K€ - 200K€", "200K€ - 300K€", 
                           "300K€ - 400K€", "400K€ - 500K€", "Plus de 500K€"],
            "nombre": [5, 15, 25, 20, 10, 5]
        })
    
    # Création d'un graphique à barres pour la distribution des prix
    fig_price_dist = px.bar(
        df_dist, 
        x="tranche_prix", 
        y="nombre", 
        title="Distribution des prix",
        labels={"tranche_prix": "Tranches de prix", "nombre": "Nombre de propriétés"},
        color_discrete_sequence=["#2c3e50"]
    )
    
    # Carte simple pour démonstration (utiliserait les emplacements réels en production)
    map_fig = go.Figure(go.Scattermapbox(
        lat=[48.8566, 48.85, 48.86, 48.87],
        lon=[2.3522, 2.34, 2.35, 2.36],
        mode='markers',
        marker=go.scattermapbox.Marker(size=14, color="#1abc9c"),
        text=["Paris", "Propriété 1", "Propriété 2", "Propriété 3"],
    ))
    
    map_fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=go.layout.mapbox.Center(lat=48.8566, lon=2.3522),
            zoom=12
        ),
        title="Carte des biens immobiliers",
        margin={"r": 0, "t": 40, "l": 0, "b": 0}
    )
    
    return dbc.Container([
        # Cartes statistiques
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Prix moyen", className="card-title"),
                        html.H2(f"{stats['prix_moyen']:,.0f} €", className="text-primary"),
                        html.P(f"Basé sur {stats['nombre_proprietes']} propriétés", className="text-muted")
                    ])
                ], className="h-100")
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Nombre de biens", className="card-title"),
                        html.H2(f"{stats['nombre_proprietes']}", className="text-success"),
                        html.P("Biens disponibles à la vente", className="text-muted")
                    ])
                ], className="h-100")
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Surface moyenne", className="card-title"),
                        html.H2("85 m²", className="text-info"),  # Données factices
                        html.P("Des propriétés disponibles", className="text-muted")
                    ])
                ], className="h-100")
            ], width=4),
        ], className="mb-4"),
        
        # Rangée des graphiques
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(figure=fig_price_dist, style={"height": "400px"})
                    ])
                ], className="h-100")
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(figure=map_fig, style={"height": "400px"})
                    ])
                ], className="h-100")
            ], width=6),
        ], className="mb-4"),
        
        # Biens récents
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Derniers biens ajoutés"),
                    dbc.CardBody([
                        html.Div(id="recent-listings", children=[
                            # Serait rempli avec des données réelles
                            dbc.ListGroup([
                                dbc.ListGroupItem([
                                    html.Div([
                                        html.H5("Appartement 3 pièces - 75 m²", className="mb-1"),
                                        html.P("320 000 € - Paris 75011", className="mb-1"),
                                        dbc.Badge("Nouveau", color="success", className="me-1"),
                                        dbc.Badge("DPE: B", color="primary"),
                                    ])
                                ], className="d-flex justify-content-between align-items-center"),
                                dbc.ListGroupItem([
                                    html.Div([
                                        html.H5("Maison 5 pièces - 120 m²", className="mb-1"),
                                        html.P("450 000 € - Lyon 69003", className="mb-1"),
                                        dbc.Badge("3 jours", color="info", className="me-1"),
                                        dbc.Badge("DPE: C", color="primary"),
                                    ])
                                ], className="d-flex justify-content-between align-items-center"),
                                dbc.ListGroupItem([
                                    html.Div([
                                        html.H5("Studio - 30 m²", className="mb-1"),
                                        html.P("180 000 € - Bordeaux 33000", className="mb-1"),
                                        dbc.Badge("1 semaine", color="warning", className="me-1"),
                                        dbc.Badge("DPE: D", color="primary"),
                                    ])
                                ], className="d-flex justify-content-between align-items-center"),
                            ])
                        ])
                    ])
                ])
            ])
        ])
    ])

def render_search():
    """Affiche l'onglet de recherche"""
    return dbc.Container([
        # Filtres de recherche
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Filtres de recherche"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Code Postal"),
                                dbc.Input(id="search-postal-code", type="text", placeholder="Ex: 75011")
                            ], width=4),
                            dbc.Col([
                                html.Label("Type de bien"),
                                dcc.Dropdown(
                                    id="search-property-type",
                                    options=[
                                        {"label": "Tous", "value": ""},
                                        {"label": "Appartement", "value": "Appartement"},
                                        {"label": "Maison", "value": "Maison"},
                                        {"label": "Studio", "value": "Studio"},
                                    ],
                                    value="",
                                    clearable=False
                                )
                            ], width=4),
                            dbc.Col([
                                html.Label("Surface minimum (m²)"),
                                dbc.Input(id="search-min-surface", type="number", placeholder="Ex: 50")
                            ], width=4),
                        ], className="mb-3"),
                        dbc.Row([
                            dbc.Col([
                                html.Label("Prix minimum (€)"),
                                dbc.Input(id="search-min-price", type="number", placeholder="Ex: 100000")
                            ], width=4),
                            dbc.Col([
                                html.Label("Prix maximum (€)"),
                                dbc.Input(id="search-max-price", type="number", placeholder="Ex: 500000")
                            ], width=4),
                            dbc.Col([
                                html.Label("Nombre minimum de pièces"),
                                dcc.Dropdown(
                                    id="search-min-rooms",
                                    options=[
                                        {"label": "Tous", "value": ""},
                                        {"label": "1 pièce", "value": 1},
                                        {"label": "2 pièces", "value": 2},
                                        {"label": "3 pièces", "value": 3},
                                        {"label": "4 pièces", "value": 4},
                                        {"label": "5 pièces et plus", "value": 5},
                                    ],
                                    value="",
                                    clearable=False
                                )
                            ], width=4),
                        ], className="mb-3"),
                        dbc.Row([
                            dbc.Col([
                                dbc.Button("Rechercher", id="search-button", color="primary", className="w-100")
                            ], width=12)
                        ])
                    ])
                ])
            ])
        ], className="mb-4"),
        
        # Résultats de recherche
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Résultats de recherche"),
                    dbc.CardBody([
                        html.Div(id="search-results", children=[
                            html.P("Utilisez les filtres ci-dessus pour rechercher des biens immobiliers.")
                        ])
                    ])
                ])
            ])
        ])
    ])

def render_analysis():
    """Affiche l'onglet d'analyse avec les données DV3F."""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Analyse DV3F"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Code INSEE"),
                                dbc.Input(id="dv3f-code-insee", type="text", placeholder="Ex: 35238")
                            ], width=4),
                            dbc.Col([
                                html.Label("Type de bien"),
                                dcc.Dropdown(
                                    id="dv3f-type-bien",
                                    options=[
                                        {"label": "Tous", "value": ""},
                                        {"label": "Maison", "value": "Maison"},
                                        {"label": "Appartement", "value": "Appartement"}
                                    ],
                                    value=""
                                )
                            ], width=4),
                            dbc.Col([
                                html.Br(),
                                dbc.Button("Analyser", id="dv3f-analyze-button", color="primary")
                            ], width=4)
                        ])
                    ])
                ])
            ])
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Évolution des prix médians"),
                    dbc.CardBody([
                        dcc.Graph(id="dv3f-evolution-graph")
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Carte des mutations récentes"),
                    dbc.CardBody([
                        dcc.Graph(id="dv3f-mutations-map")
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Statistiques DV3F"),
                    dbc.CardBody([
                        html.Div(id="dv3f-stats")
                    ])
                ])
            ])
        ])
    ])

def render_prediction():
    """Affiche l'onglet de prédiction"""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H3("Estimation de prix immobilier", className="mb-4"),
                html.P(
                    "Utilisez notre modèle d'estimation pour obtenir une évaluation du prix d'un bien immobilier. "
                    "Remplissez les caractéristiques du bien ci-dessous pour obtenir une estimation.",
                    className="mb-4"
                )
            ])
        ]),
        
        dbc.Row([
            # Formulaire de caractéristiques du bien
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Caractéristiques du bien"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Type de bien *"),
                                dcc.Dropdown(
                                    id="predict-property-type",
                                    options=[
                                        {"label": "Appartement", "value": "Appartement"},
                                        {"label": "Maison", "value": "Maison"},
                                        {"label": "Studio", "value": "Studio"},
                                    ],
                                    value="Appartement",
                                    clearable=False
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Code postal *"),
                                dbc.Input(id="predict-postal-code", type="text", placeholder="Ex: 75011")
                            ], width=6),
                        ], className="mb-3"),
                        
                        dbc.Row([
                            dbc.Col([
                                html.Label("Surface (m²) *"),
                                dbc.Input(id="predict-surface", type="number", placeholder="Ex: 75")
                            ], width=6),
                            dbc.Col([
                                html.Label("Nombre de pièces *"),
                                dbc.Input(id="predict-rooms", type="number", placeholder="Ex: 3")
                            ], width=6),
                        ], className="mb-3"),
                        
                        dbc.Row([
                            dbc.Col([
                                html.Label("Nombre de chambres"),
                                dbc.Input(id="predict-bedrooms", type="number", placeholder="Ex: 2")
                            ], width=6),
                            dbc.Col([
                                html.Label("Étage"),
                                dbc.Input(id="predict-floor", type="number", placeholder="Ex: 3")
                            ], width=6),
                        ], className="mb-3"),
                        
                        dbc.Row([
                            dbc.Col([
                                html.Label("DPE"),
                                dcc.Dropdown(
                                    id="predict-dpe",
                                    options=[
                                        {"label": "A", "value": "A"},
                                        {"label": "B", "value": "B"},
                                        {"label": "C", "value": "C"},
                                        {"label": "D", "value": "D"},
                                        {"label": "E", "value": "E"},
                                        {"label": "F", "value": "F"},
                                        {"label": "G", "value": "G"},
                                    ],
                                    value=""
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Parking"),
                                dcc.Dropdown(
                                    id="predict-parking",
                                    options=[
                                        {"label": "Oui", "value": True},
                                        {"label": "Non", "value": False},
                                    ],
                                    value=False,
                                    clearable=False
                                )
                            ], width=6),
                        ], className="mb-3"),
                        
                        dbc.Row([
                            dbc.Col([
                                html.P("* Champs obligatoires", className="text-muted mb-3"),
                                dbc.Button("Estimer le prix", id="predict-button", color="success", className="w-100")
                            ])
                        ])
                    ])
                ])
            ], width=6),
            
            # Résultats de prédiction
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Résultat de l'estimation"),
                    dbc.CardBody([
                        html.Div(id="prediction-loading", children=[
                            dbc.Spinner(color="primary", type="grow"),
                            html.P("En attente d'une estimation...", className="text-center mt-3")
                        ], style={"display": "none"}),
                        
                        html.Div(id="prediction-results", children=[
                            html.P("Remplissez le formulaire et cliquez sur 'Estimer le prix' pour obtenir une estimation.", className="text-center")
                        ]),
                        
                        html.Hr(),
                        
                        html.Div([
                            html.H4("Comment interpréter cette estimation?", className="mb-3"),
                            html.P([
                                "Notre estimation est basée sur un algorithme qui analyse les données du marché immobilier actuel. ",
                                "Il s'agit d'une approximation qui peut varier selon plusieurs facteurs non pris en compte ici comme ",
                                "l'état général du bien, les aménagements spécifiques, l'orientation, etc."
                            ]),
                            html.P([
                                "Pour une estimation plus précise, nous vous recommandons de contacter une agence FNAIM près de chez vous."
                            ]),
                            dbc.Button("Contacter une agence", color="link", className="p-0")
                        ])
                    ])
                ])
            ], width=6)
        ])
    ])

def render_dv3f():
    """Affiche l'onglet des données DV3F"""
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Analyse DV3F"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Code INSEE *"),
                                dbc.Input(
                                    id="dv3f-code-insee",
                                    type="text",
                                    placeholder="Ex: 35238",
                                    className="mb-2"
                                ),
                                html.Small(
                                    "Code INSEE de la commune (ex: 35238 pour Rennes)",
                                    className="text-muted"
                                )
                            ], width=4),
                            dbc.Col([
                                html.Label("Type de bien"),
                                dcc.Dropdown(
                                    id="dv3f-type-bien",
                                    options=[
                                        {"label": "Tous", "value": ""},
                                        {"label": "Maison", "value": "Maison"},
                                        {"label": "Appartement", "value": "Appartement"}
                                    ],
                                    value="",
                                    className="mb-2"
                                )
                            ], width=4),
                            dbc.Col([
                                html.Br(),
                                dbc.Button(
                                    "Analyser",
                                    id="dv3f-analyze-button",
                                    color="primary",
                                    className="w-100"
                                )
                            ], width=4)
                        ])
                    ])
                ])
            ])
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Évolution des prix médians"),
                    dbc.CardBody([
                        dcc.Graph(
                            id="dv3f-evolution-graph",
                            config={'displayModeBar': True}
                        )
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Carte des mutations récentes"),
                    dbc.CardBody([
                        dcc.Graph(
                            id="dv3f-mutations-map",
                            config={'displayModeBar': True}
                        )
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Statistiques DV3F"),
                    dbc.CardBody([
                        html.Div(id="dv3f-stats")
                    ])
                ])
            ])
        ])
    ])

# Callback pour la recherche de propriétés
@app.callback(
    Output("search-results", "children"),
    Input("search-button", "n_clicks"),
    [
        State("search-postal-code", "value"),
        State("search-property-type", "value"),
        State("search-min-surface", "value"),
        State("search-min-price", "value"),
        State("search-max-price", "value"),
        State("search-min-rooms", "value")
    ],
    prevent_initial_call=True
)
def update_search_results(n_clicks, postal_code, property_type, min_surface, min_price, max_price, min_rooms):
    if not n_clicks:
        return html.P("Utilisez les filtres ci-dessus pour chercher des biens immobiliers.")
    filters = {}
    if postal_code: filters["code_postal"] = postal_code
    if property_type: filters["type_habitation"] = property_type
    if min_surface: filters["surface_min"] = min_surface
    if min_price: filters["prix_min"] = min_price
    if max_price: filters["prix_max"] = max_price
    if min_rooms: filters["nb_pieces_min"] = min_rooms
    properties = get_properties(filters)
    if not properties:
        return html.P("Aucun bien trouvé.")
    property_cards = []
    for prop in properties[:10]:
        price = prop.get("prix", "N/A")
        surface = prop.get("surface", "N/A")
        rooms = prop.get("nb_pieces", "N/A")
        property_type = prop.get("type_habitation", "Bien immobilier")
        postal_code = prop.get("code_postal", "N/A")
        reference = prop.get("reference", "")
        property_cards.append(
            dbc.Card([
                dbc.Row([
                    dbc.Col([
                        html.Div(class_="property-image bg-light", style={"height": "150px", "background": "#eee"})
                    ], width=4),
                    dbc.Col([
                        dbc.CardBody([
                            html.H5(f"{property_type} - {rooms} pièces - {surface} m²"),
                            html.H4(price, className="text-primary"),
                            html.P(f"Code postal: {postal_code}"),
                            html.P(f"Réf: {reference}", className="text-muted"),
                            dbc.Button("Voir détails", color="info", size="sm")
                        ])
                    ], width=8)
                ], className="g-0")
            ], className="mb-3")
        )
    map_figure = create_map_figure(properties)
    return dbc.Row([
        dbc.Col([dcc.Graph(figure=map_figure)], width=6),
        dbc.Col([html.H5(f"{len(properties)} résultats trouvés", className="mb-3"), html.Div(property_cards)], width=6)
    ])

# Callback pour la prédiction de prix
@app.callback(
    [
        Output("prediction-loading", "style"),
        Output("prediction-results", "children")
    ],
    Input("predict-button", "n_clicks"),
    [
        State("predict-property-type", "value"),
        State("predict-postal-code", "value"),
        State("predict-surface", "value"),
        State("predict-rooms", "value"),
        State("predict-bedrooms", "value"),
        State("predict-floor", "value"),
        State("predict-dpe", "value"),
        State("predict-parking", "value")
    ],
    prevent_initial_call=True
)
def update_price_prediction(n_clicks, property_type, postal_code, surface, rooms, 
                            bedrooms, floor, dpe, parking):
    """Génère une prédiction de prix basée sur les caractéristiques d'entrée"""
    if not n_clicks:
        return {"display": "none"}, html.P("Remplissez le formulaire et cliquez sur 'Estimer le prix' pour obtenir une estimation.")
    
    # Vérification des champs obligatoires
    if not all([property_type, postal_code, surface, rooms]):
        return {"display": "none"}, html.Div([
            html.Div(className="alert alert-danger", children=[
                html.H4("Champs obligatoires manquants", className="alert-heading"),
                html.P("Veuillez remplir tous les champs marqués d'un astérisque (*) pour obtenir une estimation.")
            ])
        ])
    
    # Afficher le spinner de chargement
    loading_style = {"display": "block"}
    
    # Préparer le dictionnaire de caractéristiques pour la prédiction
    features = {
        "type_habitation": property_type,
        "code_postal": postal_code,
        "surface": float(surface) if surface else 0,
        "nb_pieces": int(rooms) if rooms else 0,
        "nb_chambres": int(bedrooms) if bedrooms else 0,
        "etage": int(floor) if floor else 0,
        "dpe_rating": dpe,
        "parking": parking
    }
    
    # Simuler un délai API
    import time
    time.sleep(1)
    
    # Obtenir la prédiction
    predicted_price = predict_price(features)
    
    # Formater le prix avec des espaces comme séparateurs de milliers
    formatted_price = f"{predicted_price:,}".replace(",", " ")
    
    # Calculer la fourchette de prix (±10%)
    min_price = int(predicted_price * 0.9)
    max_price = int(predicted_price * 1.1)
    formatted_min_price = f"{min_price:,}".replace(",", " ")
    formatted_max_price = f"{max_price:,}".replace(",", " ")
    
    # Formater le prix par m²
    price_per_sqm = int(predicted_price / float(surface)) if surface else 0
    formatted_price_per_sqm = f"{price_per_sqm:,}".replace(",", " ")
    
    result = html.Div([
        html.Div(className="text-center mb-4", children=[
            html.H3("Estimation de prix", className="mb-3"),
            html.H1(f"{formatted_price} €", className="text-success display-4 mb-2"),
            html.P(f"Fourchette de prix: {formatted_min_price} € - {formatted_max_price} €", className="text-muted"),
            html.P(f"Prix au m²: {formatted_price_per_sqm} €/m²", className="text-muted"),
        ]),
        
        dbc.Card([
            dbc.CardHeader("Récapitulatif du bien"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.P([html.Strong("Type: "), property_type]),
                        html.P([html.Strong("Code postal: "), postal_code]),
                        html.P([html.Strong("Surface: "), f"{surface} m²"])
                    ], width=6),
                    dbc.Col([
                        html.P([html.Strong("Pièces: "), rooms]),
                        html.P([html.Strong("Chambres: "), bedrooms or "Non spécifié"]),
                        html.P([html.Strong("DPE: "), dpe or "Non spécifié"])
                    ], width=6)
                ])
            ])
        ])
    ])
    
    return {"display": "none"}, result

# Mise à jour du callback DV3F
@app.callback(
    [
        Output("dv3f-evolution-graph", "figure"),
        Output("dv3f-mutations-map", "figure"),
        Output("dv3f-stats", "children")
    ],
    [Input("dv3f-analyze-button", "n_clicks")],
    [State("dv3f-code-insee", "value"),
     State("dv3f-type-bien", "value")],
    prevent_initial_call=True
)
def update_dv3f_analysis(n_clicks, code_insee, type_bien):
    """Met à jour les graphiques et statistiques DV3F."""
    if not n_clicks or not code_insee:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Aucune donnée à afficher",
            xaxis={"visible": False},
            yaxis={"visible": False},
            annotations=[{
                "text": "Veuillez saisir un code INSEE et cliquer sur Analyser",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
        return empty_fig, empty_fig, "Veuillez saisir un code INSEE"
    
    # Récupération des données d'évolution
    evolution_data = get_dv3f_evolution(code_insee, type_bien)
    if evolution_data:
        df_evolution = pd.DataFrame(evolution_data)
        fig_evolution = px.line(
            df_evolution,
            x="annee",
            y=["prix_median", "prix_m2_median"],
            title="Évolution des prix",
            labels={
                "value": "Prix (€)", 
                "annee": "Année",
                "variable": "Indicateur",
                "prix_median": "Prix médian",
                "prix_m2_median": "Prix/m²"
            }
        )
        fig_evolution.update_layout(
            legend_title_text="Indicateur",
            hovermode="x unified"
        )
    else:
        fig_evolution = go.Figure()
        fig_evolution.update_layout(
            title="Aucune donnée d'évolution disponible",
            annotations=[{
                "text": "Données non disponibles pour ce code INSEE",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
    
    # Récupération des mutations récentes
    mutations_data = get_dv3f_mutations(code_insee=code_insee)
    if mutations_data:
        fig_mutations = create_map_mutations(mutations_data)
    else:
        fig_mutations = go.Figure()
        fig_mutations.update_layout(
            title="Aucune mutation disponible",
            annotations=[{
                "text": "Pas de mutations récentes pour ce code INSEE",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 20}
            }]
        )
    
    # Statistiques
    stats_data = get_dv3f_data(code_insee, type_bien)
    if stats_data:
        try:
            stats = pd.DataFrame(stats_data).iloc[-1]  # Dernière année
            stats_display = html.Div([
                html.H4(f"Statistiques pour {stats['nom_commune']} ({stats['annee']})", className="mb-4"),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("Maisons individuelles", className="card-title"),
                                html.P([
                                    html.Strong("Nombre de transactions: "),
                                    f"{stats['nbtrans_cod111'] or 0:,}".replace(",", " ")
                                ]),
                                html.P([
                                    html.Strong("Prix médian: "),
                                    f"{stats['prix_median_cod111'] or 0:,.0f}€".replace(",", " ")
                                ]),
                                html.P([
                                    html.Strong("Prix/m² médian: "),
                                    f"{stats['prix_m2_median_cod111'] or 0:,.0f}€/m²".replace(",", " ")
                                ])
                            ])
                        ])
                    ], width=6),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("Appartements", className="card-title"),
                                html.P([
                                    html.Strong("Nombre de transactions: "),
                                    f"{stats['nbtrans_cod121'] or 0:,}".replace(",", " ")
                                ]),
                                html.P([
                                    html.Strong("Prix médian: "),
                                    f"{stats['prix_median_cod121'] or 0:,.0f}€".replace(",", " ")
                                ]),
                                html.P([
                                    html.Strong("Prix/m² médian: "),
                                    f"{stats['prix_m2_median_cod121'] or 0:,.0f}€/m²".replace(",", " ")
                                ])
                            ])
                        ])
                    ], width=6)
                ])
            ])
        except Exception as e:
            print(f"Erreur lors du traitement des statistiques: {e}")
            stats_display = html.Div([
                html.P("Erreur lors du traitement des statistiques", className="text-danger")
            ])
    else:
        stats_display = html.Div([
            html.P("Aucune donnée statistique disponible pour ce code INSEE", className="text-muted")
        ])
    
    return fig_evolution, fig_mutations, stats_display

# Lancer l'application
if __name__ == "__main__":
    app.run_server(debug=True)
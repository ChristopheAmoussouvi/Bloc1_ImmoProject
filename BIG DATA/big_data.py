import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# --- Fonction pour exécuter les requêtes SPARQL ---
def run_sparql_query(query):
    """
    Exécute une requête SPARQL sur Wikidata et retourne les résultats.

    Args:
        query (str): La requête SPARQL à exécuter.

    Returns:
        dict: Les résultats de la requête au format JSON, ou None en cas d'erreur.
    """
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        return results
    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête SPARQL : {e}")
        return None

# --- Fonction pour récupérer les données d'une ville ---
def get_city_data(city_name):
    """
    Récupère des données d'une ville à partir de Wikidata.

    Args:
        city_name (str): Le nom de la ville.

    Returns:
        pandas.DataFrame: Un DataFrame contenant les données de la ville, 
                         ou None en cas d'erreur.
    """
    # --- Requête principale (population, superficie, densité) ---
    query_main = f"""
    SELECT ?population ?area ?date WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city p:P1082 ?populationNode.
      ?populationNode ps:P1082 ?population.
      ?populationNode pq:P585 ?date.
      ?city wdt:P2046 ?area.
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    results_main = run_sparql_query(query_main)

    if results_main and results_main['results']['bindings']:
        population = int(results_main['results']['bindings'][0]['population']['value'])
        area = float(results_main['results']['bindings'][0]['area']['value'])
        date = results_main['results']['bindings'][0]['date']['value']
        density = population / area
    else:
        population, area, date, density = None, None, None, None
        print(f"Erreur : Impossible de récupérer les données principales pour {city_name}.")

    # --- Requêtes pour les informations supplémentaires ---

    # Villes voisines
    query_neighbors = f"""
    SELECT ?neighbor ?neighborLabel WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city wdt:P47 ?neighbor.
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "fr". }}
    }}
    """
    results_neighbors = run_sparql_query(query_neighbors)
    neighbors = [item['neighborLabel']['value'] for item in results_neighbors['results']['bindings']] if results_neighbors else []

    # Code postal
    query_postal_code = f"""
    SELECT ?postalCode WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city wdt:P281 ?postalCode.
    }}
    """
    results_postal_code = run_sparql_query(query_postal_code)
    postal_code = results_postal_code['results']['bindings'][0]['postalCode']['value'] if results_postal_code and results_postal_code['results']['bindings'] else None

    # Coordonnées
    query_coordinates = f"""
    SELECT ?location WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city wdt:P625 ?location.
    }}
    """
    results_coordinates = run_sparql_query(query_coordinates)
    coordinates = results_coordinates['results']['bindings'][0]['location']['value'] if results_coordinates and results_coordinates['results']['bindings'] else None

    # Gentilé
    query_demonym = f"""
    SELECT ?demonym WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city wdt:P1549 ?demonym.
      FILTER(LANG(?demonym) = "fr")
    }}
    """
    results_demonym = run_sparql_query(query_demonym)
    demonym = results_demonym['results']['bindings'][0]['demonym']['value'] if results_demonym and results_demonym['results']['bindings'] else None

    # Région (Label)
    query_region_label = f"""
    SELECT ?regionLabel WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515;  # instance of or subclass of city
            rdfs:label "{city_name}"@fr. # city label in French
      ?city wdt:P131 ?region.
      ?region rdfs:label ?regionLabel.  # Get the region's label
      FILTER(LANG(?regionLabel) = "fr"). # Filter for French labels
    }}
    LIMIT 1
    """
    results_region_label = run_sparql_query(query_region_label)
    region_label = results_region_label['results']['bindings'][0]['regionLabel']['value'] if results_region_label and results_region_label['results']['bindings'] else None

    # --- Création du DataFrame final ---
    data = [{
        'ville': city_name,
        'population': population,
        'surface': area,
        'date': date,
        'densité': density,
        'villes_voisines': ", ".join(neighbors),
        'code_postale': postal_code,
        'coordonnees': coordinates,
        'demonym': demonym,
        'region_label': region_label
    }]
    df = pd.DataFrame(data)
    return df

# --- Utilisation de la fonction ---
city_data = get_city_data("Rennes")  # Pour la ville de Rennes
print(city_data)
city_data.to_csv('city_data.csv', index=False)
print("Les résultats ont été sauvegardés dans 'city_data.csv'")
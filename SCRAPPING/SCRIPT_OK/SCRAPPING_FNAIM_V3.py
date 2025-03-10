import re
import pandas as pd
import requests
import time
import datetime
import random
import logging
import os
from tqdm import tqdm
from bs4 import BeautifulSoup
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import PoolManager
from lxml import etree, html
from urllib.parse import urlparse  

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

timestamp = datetime.datetime.now()

# Dossier pour les sauvegardes intermédiaires
BACKUP_DIR = "scraping_backups"
os.makedirs(BACKUP_DIR, exist_ok=True)

####
#    FONCTIONS UTILITAIRES    #
####
def extract_numbers(text):
    """Extrait le premier nombre entier d'une chaîne de caractères."""
    if not text:
        return None
    numbers = re.findall(r'\d+', text)
    if numbers:
        return int(numbers[0])
    return None

def create_session_with_retry(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 504, 429)):
    """
    Crée une session avec une stratégie de nouvelle tentative optimisée avec urllib3.
    """
    # Configuration avancée pour urllib3 et la gestion des connexions
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
        # Nouvelles options pour améliorer la robustesse
        connect=retries,
        read=retries,
        redirect=5
    )
    
    # Créer un adaptateur avec le gestionnaire de connexions optimisé
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=20,
        pool_block=False
    )
    
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Ajout d'en-têtes pour simuler un navigateur réel
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    })
    
    return session

def save_progress(df_annonces, df_agences, prefix='interim'):
    """Sauvegarde les données collectées jusqu'à présent."""
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    annonces_file = f"{BACKUP_DIR}/{prefix}_annonces_{timestamp_str}.csv"
    agences_file = f"{BACKUP_DIR}/{prefix}_agences_{timestamp_str}.csv"
    
    df_annonces.to_csv(annonces_file, index=False)
    df_agences.to_csv(agences_file, index=False)
    logger.info(f"Progression sauvegardée : {len(df_annonces)} annonces et {len(df_agences)} agences")

####
#    SCRAPING DES DÉTAILS D'ANNONCE
####
def scrapping_annonce(url_annonce, session=None, timeout=30):
    """
    Scrape les détails d'une annonce immobilière et de l'agence associée à partir d'une URL.
    Utilise lxml pour un parsing plus rapide.
    """
    if session is None:
        session = create_session_with_retry()
    
    try:
        annonce_fiche_response = session.get(url_annonce, timeout=timeout)
        annonce_fiche_response.raise_for_status()
        
        # Utilisation de lxml pour un parsing plus rapide
        annonce_fiche_html = annonce_fiche_response.content
        annonce_fiche_soup = BeautifulSoup(annonce_fiche_html, "lxml")
        
        # Version lxml directe pour certaines extractions complexes
        tree = html.fromstring(annonce_fiche_html)
        
        # Titre, prix et référence
        titre_element = annonce_fiche_soup.find('h1', class_='titreFiche')
        titre = titre_element.get_text(strip=True) if titre_element else None
        
        price_element = annonce_fiche_soup.find('span', itemprop='price')
        prix = price_element.get_text(strip=True) if price_element else None
        
        ref_element = annonce_fiche_soup.find('meta', itemprop='productid')
        ref = ref_element['content'] if ref_element else None
        
        # Utilisation de XPath pour extractions spécifiques
        lieu_xpath = tree.xpath('//li[contains(@class, "picto lieu")]/b/text()')
        lieu = lieu_xpath[0].strip() if lieu_xpath else None
        
        # Extraire juste le code postal 
        if lieu:
            match = re.search(r'(\d{5})', lieu)
            lieu = match.group(1) if match else None
        
        # Surface avec XPath
        surface_xpath = tree.xpath('//li[contains(@class, "picto surface")]/b/text()')
        surface = surface_xpath[0].strip() if surface_xpath else None
        surface_int = extract_numbers(surface) if surface else None
        
        # Nombre de pièces avec XPath
        pieces_xpath = tree.xpath('//li[contains(@class, "picto pieces")]/b/text()')
        nbr_pieces = pieces_xpath[0].strip() if pieces_xpath else None
        nb_pieces = extract_numbers(nbr_pieces) if nbr_pieces else None
        
        # Type d'habitation - combinaison BS4 et XPath
        habit_type_element = annonce_fiche_soup.find('label', string="Type d'habitation : ")
        if habit_type_element:
            habit_type_text = habit_type_element.find_next_sibling(text=True)
            habit_type = habit_type_text.strip() if habit_type_text else None
        else:
            # Essayer avec les méta données
            habit_type_meta = annonce_fiche_soup.find('meta', itemprop='model')
            habit_type = habit_type_meta['content'] if habit_type_meta else None
            
            # Alternative avec XPath si toujours pas de résultat
            if not habit_type:
                habit_xpath = tree.xpath('//meta[@itemprop="model"]/@content')
                habit_type = habit_xpath[0] if habit_xpath else None
        
        # DPE et GES - Performance énergétique (utilisation avancée de XPath)
        dpe_text = None
        dpe_elements = tree.xpath('//li[contains(text(), "DPE")] | //li[.//label[contains(text(), "DPE")]]')
        if dpe_elements:
            dpe_text = dpe_elements[0].text_content().strip()
        
        if dpe_text:
            # Extraire la lettre du DPE
            dpe_match = re.search(r'DPE\s*:\s*([A-G])', dpe_text)
            if not dpe_match:
                dpe_match = re.search(r'DPE\s*[^\w]*([A-G])', dpe_text)
            dpe_rating = dpe_match.group(1) if dpe_match else None
            
            # Extraire la consommation
            consumption_match = re.search(r'(\d+)\s*kWh/m[²²]\s*an', dpe_text)
            dpe_consumption = int(consumption_match.group(1)) if consumption_match else None
        else:
            dpe_rating = None
            dpe_consumption = None
        
        # GES avec XPath
        ges_text = None
        ges_elements = tree.xpath('//li[contains(text(), "GES")] | //li[.//label[contains(text(), "GES")]]')
        if ges_elements:
            ges_text = ges_elements[0].text_content().strip()
        elif dpe_text and "GES" in dpe_text:
            ges_text = dpe_text
            
        if ges_text:
            # Extraire la lettre du GES
            ges_rang_match = re.search(r'GES\s*:\s*([A-G])', ges_text)
            if not ges_rang_match:
                ges_rang_match = re.search(r'GES\s*[^\w]*([A-G])', ges_text)
            ges_rang = ges_rang_match.group(1) if ges_rang_match else None
            
            # Extraire l'émission
            emission_match = re.search(r'(\d+)\s*kgCO2/m[²²].an', ges_text)
            ges_emission = int(emission_match.group(1)) if emission_match else None
        else:
            ges_rang = None
            ges_emission = None
        
        # Estimation des dépenses énergétiques
        depenses_elements = tree.xpath('//li[.//label[contains(text(), "Montant estimé des dépenses")]]')
        if depenses_elements:
            depenses_text = depenses_elements[0].text_content().strip()
            # Extraire les montants min et max
            montants_match = re.search(r'Entre\s*(\d+)\s*€\s*TTC\s*/\s*an\s*et\s*(\d+)\s*€\s*TTC\s*/\s*an', depenses_text)
            if montants_match:
                depenses_min = int(montants_match.group(1))
                depenses_max = int(montants_match.group(2))
            else:
                depenses_min = None
                depenses_max = None
            
            # Date de référence des prix
            date_ref_match = re.search(r'Date de référence des prix[^:]*:\s*(\d{2}/\d{2}/\d{4})', depenses_text)
            date_ref_prix = date_ref_match.group(1) if date_ref_match else None
        else:
            depenses_min = None
            depenses_max = None
            date_ref_prix = None
        
        # Caractéristiques supplémentaires
        # Nombre de chambres avec XPath
        chambres_xpath = tree.xpath('//li[contains(text(), "Nombre de chambres")] | //label[contains(text(), "Nombre de chambres")]/following-sibling::text()[1]')
        if chambres_xpath:
            nb_chambres = extract_numbers(chambres_xpath[0])
        else:
            nb_chambres = None
        
        # Description
        description_element = annonce_fiche_soup.find("p", itemprop="description")
        description = description_element.get_text(strip=True) if description_element else None
        
        # Parking
        parking_xpath = tree.xpath('//li[contains(@class, "picto parking")]/b/text()')
        parking_bool = False
        if parking_xpath:
            parking_text = parking_xpath[0].strip()
            parking_bool = True if parking_text == "Oui" else False
    
        # Extraction des liens d'images avec XPath
        images_links = []
        image_elements = tree.xpath('//div[@id="diapo_annonce"]//a[contains(@class, "imageAnnonce")]/@href')
        if image_elements:
            images_links = image_elements
        
        # Si pas d'images trouvées, essayer les métadonnées
        if not images_links:
            meta_image = tree.xpath('//meta[@itemprop="image"]/@content')
            if meta_image:
                images_links.append(meta_image[0])
    
        # Informations sur l'agence
        agency_id = None
        agency_name = None
        agency_address = None
        agency_url = None
        agency_phone = None
        agency_siret = None
        agency_card_number = None
        agency_legal_reps = None
        
        # Extraction des données de l'agence avec XPath
        agency_name_xpath = tree.xpath('//div[contains(@class, "caracteristique agence")]//div[contains(@class, "libelle")]/a/text()')
        if agency_name_xpath:
            agency_name = agency_name_xpath[0].strip()
            
        agency_address_xpath = tree.xpath('//div[contains(@class, "caracteristique agence")]//p[contains(@class, "addresse")]/text()')
        if agency_address_xpath:
            agency_address = agency_address_xpath[0].strip()
            
        agency_url_xpath = tree.xpath('//div[contains(@class, "caracteristique agence")]//div[contains(@class, "libelle")]/a/@href')
        if agency_url_xpath:
            agency_url = agency_url_xpath[0]
            agency_id_match = re.search(r'/agence-immobiliere/(\d+)/', agency_url)
            if agency_id_match:
                agency_id = agency_id_match.group(1)
        
        # Construction de l'URL complète pour l'agence
        from urllib.parse import urlparse
        parsed_url = urlparse(url_annonce)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        full_agency_url = base_url + agency_url if agency_url else None
        
        # Requête vers la page de l'agence - Optionnelle, peut être désactivée si nécessaire
        agency_details = True
        if full_agency_url and agency_details:
            try:
                # Utilisation d'un timeout plus long pour les requêtes d'agence
                agency_response = session.get(full_agency_url, timeout=timeout * 1.5)
                agency_tree = html.fromstring(agency_response.content)
                
                # Récupération du numéro de téléphone avec XPath
                phone_xpath = agency_tree.xpath('//span[@id="agence_call"]/text()')
                if phone_xpath:
                    agency_phone = phone_xpath[0].strip()
                
                # Récupération des informations complémentaires
                info_items = agency_tree.xpath('//div[contains(@class, "caracteristique tab-left")]//li')
                for item in info_items:
                    item_text = item.text_content().strip()
                    if 'SIRET' in item_text:
                        agency_siret = item_text.split(':', 1)[1].strip() if ':' in item_text else item_text
                    elif 'Carte N°' in item_text:
                        agency_card_number = item_text.split(':', 1)[1].strip() if ':' in item_text else item_text
                    elif 'Représentants légaux' in item_text:
                        agency_legal_reps = item_text.split(':', 1)[1].strip() if ':' in item_text else item_text
            
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération des détails de l'agence {full_agency_url}: {e}")
        
        # Construction des DataFrames
        data_annonce = {
            'titre': titre,
            'prix': prix,
            'reference': ref,
            'code_postal': lieu,
            'surface': surface_int,
            'nb_pieces': nb_pieces,
            'type_habitation': habit_type,
            'dpe_rating': dpe_rating,
            'dpe_consumption': dpe_consumption,
            'ges_rating': ges_rang,
            'ges_emission': ges_emission,
            'nb_chambres': nb_chambres,
            'description': description,
            'parking': parking_bool,
            'url': url_annonce,
            'images': images_links,
            'depenses_energie_min': depenses_min,
            'depenses_energie_max': depenses_max,
            'date_ref_prix_energie': date_ref_prix,
            'agency_id': agency_id,
            'date_scrape': timestamp
        }
        
        data_agence = {
            'agency_id': agency_id,
            'agency_name': agency_name,
            'agency_address': agency_address,
            'agency_url': agency_url,
            'agency_phone': agency_phone,
            'agency_siret': agency_siret,
            'agency_card_number': agency_card_number,
            'agency_legal_reps': agency_legal_reps,
            'date_scrape': timestamp
        }
        
        df_annonce = pd.DataFrame([data_annonce])
        df_agence = pd.DataFrame([data_agence])
        
        return df_annonce, df_agence
    
    except Exception as e:
        logger.error(f"Erreur dans scrapping_annonce pour {url_annonce}: {e}")
        raise

####
#    EXTRACTION DES URLS DES ANNONCES    #
####
def scrapping_urls(url, session=None, timeout=20):
    """
    Extrait les URLs des annonces immobilières à partir d'une page de résultats FNAIM.
    Version robuste avec détection multi-méthodes.
    """
    if session is None:
        session = create_session_with_retry()
    
    try:
        urls = set()
        fnaim_url = "https://www.fnaim.fr"
        
        # Obtention de la page avec un User-Agent plus réaliste
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            'Referer': 'https://www.fnaim.fr/',
            'Connection': 'keep-alive',
        }
        session.headers.update(headers)
        
        response = session.get(url, timeout=timeout)
        status_code = response.status_code
        
        if status_code != 200:
            logger.warning(f"Status code non-200 reçu: {status_code}")
            return [], status_code
        
        # Sauvegarde du HTML pour analyse
        with open("debug_fnaim.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info(f"HTML de la page sauvegardé pour analyse")
        
        # Méthode 1: Sélecteur XPath original
        tree = html.fromstring(response.content)
        link_elements = tree.xpath('//a[@class="linkAnnonce"]/@href')
        
        # Méthode 2: BeautifulSoup pour plus de robustesse
        if not link_elements:
            soup = BeautifulSoup(response.content, "lxml")
            links = soup.find_all('a', class_='linkAnnonce')
            if links:
                link_elements = [link['href'] for link in links if link.has_attr('href')]
                
        # Méthode 3: Sélecteurs alternatifs XPath
        if not link_elements:
            # Recherche des liens dans les blocs d'annonces
            link_elements = tree.xpath('//div[contains(@class, "liste-biens")]//a[contains(@href, "/annonce-immobiliere/")]/@href')
            
            # Recherche plus large par attribut href
            if not link_elements:
                link_elements = tree.xpath('//a[contains(@href, "/annonce-immobiliere/")]/@href')
                
            # Essai avec CSS selecteurs via BeautifulSoup
            if not link_elements:
                soup = BeautifulSoup(response.content, "lxml") if not 'soup' in locals() else soup
                all_links = soup.select('a[href*="/annonce-immobiliere/"]')
                link_elements = [link.get('href') for link in all_links if link.get('href')]
        
        # Traitement des liens trouvés
        for link in link_elements:
            if "#AGE_CONTACT" not in link:
                if link.startswith('/'):
                    urls.add(fnaim_url + link)
                else:
                    urls.add(link)
        
        filtered_urls = list(urls)
        logger.info(f"Status code: {status_code}, Nombre d'annonces trouvées: {len(filtered_urls)}")
        
        # Log d'un échantillon des URLs trouvées
        if filtered_urls and len(filtered_urls) > 0:
            sample = filtered_urls[:min(2, len(filtered_urls))]
            logger.info(f"Exemples d'URLs: {sample}")
            
        return filtered_urls, status_code
    
    except Exception as e:
        logger.error(f"Erreur dans scrapping_urls pour {url}: {e}")
        return [], 0

####
#    FONCTION DE RETRY POUR UN SCRAPING D'ANNONCE    #
####
def fetch_announcement(url_annonce, session, max_retries=3, base_timeout=30):
    """
    Tente de scraper une annonce avec plusieurs tentatives en cas d'erreur.
    Utilise urllib3 via la session pour gérer les retries.
    """
    for attempt in range(max_retries):
        try:
            # Augmenter le timeout à chaque tentative
            current_timeout = base_timeout * (attempt + 1)
            return scrapping_annonce(url_annonce, session, timeout=current_timeout)
        except Exception as e:
            logger.warning(f"Erreur lors du scraping de {url_annonce} - tentative {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                # Attente exponentielle avec élément aléatoire
                sleep_time = (2 ** attempt) + random.uniform(1, 3)
                logger.info(f"Attente de {sleep_time:.2f} secondes avant la nouvelle tentative")
                time.sleep(sleep_time)
    
    logger.error(f"Abandon de {url_annonce} après {max_retries} tentatives.")
    return pd.DataFrame(), pd.DataFrame()

####
#    GESTION DES TÂCHES PARALLÈLES    #
####
def process_page_urls(urls_annonces, session, max_workers=5, max_retries=3):
    """
    Traite une liste d'URLs d'annonces en parallèle avec urllib3 et lxml.
    """
    all_annonces = []
    all_agences = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Créer un dict de futures pour traiter les annonces en parallèle
        future_to_url = {
            executor.submit(fetch_announcement, url, session, max_retries): url
            for url in urls_annonces
        }
        
        # Traiter les résultats au fur et à mesure qu'ils arrivent
        for future in tqdm(concurrent.futures.as_completed(future_to_url), 
                          total=len(future_to_url), 
                          desc="Traitement des annonces"):
            url = future_to_url[future]
            try:
                df_annonce, df_agence = future.result()
                if not df_annonce.empty:
                    all_annonces.append(df_annonce)
                if not df_agence.empty and df_agence['agency_id'].iloc[0] is not None:
                    all_agences.append(df_agence)
            except Exception as e:
                logger.error(f"Erreur non gérée pour {url}: {e}")
    
    # Combiner les résultats avec pandas
    df_annonces = pd.concat(all_annonces, ignore_index=True) if all_annonces else pd.DataFrame()
    df_agences = pd.concat(all_agences, ignore_index=True) if all_agences else pd.DataFrame()
    
    # Dédupliquer les agences
    if not df_agences.empty:
        df_agences = df_agences.drop_duplicates(subset='agency_id', keep='first')
    
    return df_annonces, df_agences

####
#    SCRAPING PRINCIPAL MULTI-PAGES AVEC EXÉCUTION PARALLÈLE    #
####
def scrapping_fnaim(base_url, max_workers=5, max_retries=3, max_pages=None, save_interval=5):
    """
    Scrappe toutes les annonces immobilières de la FNAIM de manière optimisée avec lxml et urllib3.
    
    Args:
        base_url (str): URL de base pour la recherche
        max_workers (int): Nombre maximum de workers pour le traitement parallèle
        max_retries (int): Nombre maximum de tentatives par URL
        max_pages (int): Nombre maximum de pages à scraper (None = illimité)
        save_interval (int): Intervalle de sauvegarde en nombre de pages
    
    Returns:
        tuple: (DataFrame des annonces, DataFrame des agences)
    """
    page_number = 1
    all_annonces_df = pd.DataFrame()
    all_agences_df = pd.DataFrame()
    all_urls_processed = set()
    
    # Création d'une session partagée avec retry optimisée
    session = create_session_with_retry(retries=max_retries)
    
    try:
        while max_pages is None or page_number <= max_pages:
            url_page = f"{base_url}&ip={page_number}"
            
            # 1. Extraire les URLs des annonces de la page courante
            urls_annonces, status_code = scrapping_urls(url_page, session)
            
            # Vérifier si la page contient des annonces
            if not urls_annonces or status_code != 200:
                logger.info(f"Aucune annonce trouvée ou erreur à la page {page_number} (status: {status_code}). Fin du scraping.")
                break
            
            # Filtrer les URLs déjà traitées
            urls_to_process = [url for url in urls_annonces if url not in all_urls_processed]
            
            if not urls_to_process:
                logger.info(f"Toutes les annonces de la page {page_number} ont déjà été traitées.")
                page_number += 1
                continue
            
            logger.info(f"Page {page_number}: {len(urls_to_process)}/{len(urls_annonces)} nouvelles annonces à traiter")
            
            # 2. Traiter les annonces en parallèle
            df_annonces_page, df_agences_page = process_page_urls(
                urls_to_process, session, max_workers, max_retries
            )
            
            # 3. Mettre à jour les URLs traitées
            all_urls_processed.update(urls_to_process)
            
            # 4. Combiner les résultats
            if not df_annonces_page.empty:
                all_annonces_df = pd.concat([all_annonces_df, df_annonces_page], ignore_index=True)
            
            if not df_agences_page.empty:
                # Ajouter uniquement les nouvelles agences
                if all_agences_df.empty:
                    all_agences_df = df_agences_page
                else:
                    new_agencies = df_agences_page[~df_agences_page['agency_id'].isin(all_agences_df['agency_id'])]
                    all_agences_df = pd.concat([all_agences_df, new_agencies], ignore_index=True)
            
            # 5. Sauvegarder périodiquement
            if page_number % save_interval == 0:
                save_progress(all_annonces_df, all_agences_df, f"page_{page_number}")
            
            # 6. Passer à la page suivante
            page_number += 1
            
            # 7. Pause aléatoire entre les pages pour éviter d'être détecté
            sleep_time = random.uniform(1.5, 3.0)
            logger.info(f"Attente de {sleep_time:.2f} secondes avant la page suivante")
            time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        logger.warning("Interruption utilisateur. Sauvegarde des données collectées...")
        save_progress(all_annonces_df, all_agences_df, "interrupt")
    
    except Exception as e:
        logger.error(f"Erreur lors du scraping: {e}")
        save_progress(all_annonces_df, all_agences_df, "error")
    
    finally:
        # Toujours sauvegarder à la fin
        save_progress(all_annonces_df, all_agences_df, "final")
        
        logger.info(f"Scraping terminé. {len(all_annonces_df)} annonces et {len(all_agences_df)} agences récupérées.")
        return all_annonces_df, all_agences_df

####
#    EXÉCUTION PRINCIPALE    #
####
if __name__ == "__main__":
    # Configuration des paramètres optimisés pour le scraping
    base_url = "https://www.fnaim.fr/17-acheter.htm?TRANSACTION=1&localites=%5B%7B%22id%22%3A%221213%22%2C%22type%22%3A%223%22%2C%22label%22%3A%22RENNES+(35000)%22%2C%22insee%22%3A%2235238%22%7D%5D&TYPE%5B%5D=1&TYPE%5B%5D=2&NB_PIECES%5B%5D=&NB_PIECES%5B%5D=&SURFACE%5B%5D=&SURFACE%5B%5D=&PRIX%5B%5D=&PRIX%5B%5D=&NB_CHAMBRES%5B%5D=&NB_CHAMBRES%5B%5D=&SURFACE_TERRAIN%5B%5D=&SURFACE_TERRAIN%5B%5D=&op=CEN_VTE_PRIX_VENTE+asc%2CTRI_PRIX+asc%2CCEN_MDT_DTE_CREATION+desc&cp=b7c1074e5c0678bdbb36&mp=12&lat=48.115981245818375&lng=-1.6880820000176&zoom=12&ip=1"
    
    # Pour limiter le nombre de pages à scraper pendant les tests
    max_pages = None  # Définir une valeur (ex: 5) pour limiter, None pour tout scraper
    
    # Paramètres optimisés pour éviter les timeouts et utiliser lxml et urllib3 efficacement
    df_annonces, df_agences = scrapping_fnaim(
        base_url=base_url,
        max_workers=3,  # Réduit pour minimiser les problèmes de connexion
        max_retries=4,  # Optimisé pour urllib3
        max_pages=max_pages,
        save_interval=2  # Sauvegardes fréquentes
    )
    
    # Enregistrer les données dans des fichiers CSV
    df_annonces.to_csv('annonces_fnaim.csv', index=False)
    df_agences.to_csv('agences_fnaim.csv', index=False)
    
    logger.info("Fichiers CSV créés avec succès !")
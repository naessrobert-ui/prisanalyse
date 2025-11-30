from flask import Blueprint, render_template, request, session, redirect, url_for
import requests
import re
import json
from collections import defaultdict
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# --- Konfigurasjon ---
MAX_WORKERS = 10
FINN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7",
}

# NB: På sikt bør denne hentes fra .env, men jeg lar den stå som hos deg nå
MIN_API_NOKKEL = "094532a8-2343-4b4a-93f7-e284b1e0ec85"

# --- Blueprint ---
bil_import_bp = Blueprint(
    "bil_import",            # blueprint-navn
    __name__,
    template_folder="templates",
    static_folder="static",
)

# --- Hjelpefunksjoner ---

def hent_annonser_fra_søk(søk_url, max_biler):
    alle_biler = []
    antall_sider = (max_biler // 50) + 1
    with requests.Session() as session_req:
        for page_number in range(1, antall_sider + 1):
            if len(alle_biler) >= max_biler:
                break
            paginert_url = re.sub(r'&page=\d+', '', søk_url) + f"&page={page_number}"
            try:
                resp = session_req.get(paginert_url, headers=FINN_HEADERS, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'lxml')
                ad_links = soup.select("a[href*='/mobility/item/']")
                if not ad_links:
                    break
                for link in ad_links:
                    href = link.get("href")
                    if href and not href.startswith('https://'):
                        href = "https://www.finn.no" + href
                    finnkode_match = re.search(r"finnkode=(\d+)", href) or re.search(r"/item/(\d+)", href)
                    if finnkode_match:
                        finnkode = finnkode_match.group(1)
                        if not any(b['finnkode'] == finnkode for b in alle_biler):
                            alle_biler.append({"finnkode": finnkode, "url": href})
                            if len(alle_biler) >= max_biler:
                                break
            except requests.RequestException:
                continue
    return alle_biler


def hent_detaljert_info_fra_annonse(bil_info, session_req):
    try:
        response = session_req.get(bil_info['url'], headers=FINN_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Hent Totalpris
        totalpris_p = soup.find('p', string='Totalpris')
        if totalpris_p and totalpris_p.find_next_sibling('h2') and totalpris_p.find_next_sibling('h2').find('span'):
            bil_info['pris'] = int(
                "".join(filter(str.isdigit, totalpris_p.find_next_sibling('h2').find('span').get_text()))
            )

        # Hent selger fra JSON-data
        seller_name = None
        try:
            data_script = soup.find('script', {'data-company-profile-data': True})
            if data_script:
                json_data = json.loads(data_script.string)
                seller_name = get_nested_safe(json_data, ['companyProfile', 'orgName'])
        except (json.JSONDecodeError, TypeError):
            seller_name = None

        # Fallback for privat selger
        if not seller_name or seller_name == 'Mangler':
            private_seller_div = soup.find('div', {'data-testid': 'private-seller-name'})
            if private_seller_div:
                seller_name = private_seller_div.get_text(strip=True)

        if seller_name and seller_name != 'Mangler':
            bil_info['selger'] = " ".join(seller_name.split())

        # Hent info fra "Spesifikasjoner"
        spec_header = soup.find(lambda tag: tag.name in ['h2', 'h3'] and "Spesifikasjoner" in tag.get_text())
        if spec_header and spec_header.find_next_sibling('dl'):
            spec_list = spec_header.find_next_sibling('dl')
            reg_nr, chassis_nr = None, None
            for dt in spec_list.find_all('dt'):
                dd = dt.find_next_sibling('dd')
                if dd:
                    key, val = dt.get_text(strip=True), dd.get_text(strip=True)
                    if "Kilometerstand" in key:
                        bil_info['km'] = int("".join(filter(str.isdigit, val)))
                    elif "1. gang registrert" in key:
                        bil_info['reg_norge'] = val
                    elif "Registreringsnr." in key:
                        reg_nr = val
                    elif "Chassis nr. (VIN)" in key:
                        chassis_nr = val
            bil_info['identifikator'] = reg_nr if reg_nr else chassis_nr

        return bil_info
    except requests.RequestException:
        return bil_info


def hent_kjøretøydata_svv(identifikator, session_req):
    if not identifikator:
        return None
    identifikator_clean = identifikator.strip().upper().replace(" ", "")
    query_param = "kjennemerke" if len(identifikator_clean) == 7 and identifikator_clean[:2].isalpha() else "understellsnummer"
    url = (
        "https://www.vegvesen.no/ws/no/vegvesen/kjoretoy/felles/datautlevering/"
        f"enkeltoppslag/kjoretoydata?{query_param}={identifikator_clean}"
    )
    headers = {"SVV-Authorization": f"Apikey {MIN_API_NOKKEL}"}
    try:
        response = session_req.get(url, headers=headers, timeout=10)
        if response.status_code == 200 and response.json().get("kjoretoydataListe"):
            return response.json()["kjoretoydataListe"][0]
    except requests.RequestException:
        pass
    return None


def get_nested_safe(data, keys, default='Mangler'):
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int) and current:
            try:
                current = current[key]
            except IndexError:
                return default
        else:
            return default
        if current is None:
            return default
    return current if current is not None else default


@bil_import_bp.app_context_processor
def utility_processor():
    # gjør get_nested_safe tilgjengelig i Jinja-templates
    return dict(get_nested_safe=get_nested_safe)


# --- Ruter (nå på blueprinten) ---

@bil_import_bp.route('/')
def index():
    return render_template('index.html')


@bil_import_bp.route('/scan', methods=['POST'])
def scan():
    søk_url = request.form['url']
    max_biler = int(request.form.get('max_biler', 200))

    bil_liste_fra_søk = hent_annonser_fra_søk(søk_url, max_biler)
    biler_med_detaljer = []
    with requests.Session() as s, ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        fremtidige_biler = {executor.submit(hent_detaljert_info_fra_annonse, bil, s) for bil in bil_liste_fra_søk}
        for future in as_completed(fremtidige_biler):
            biler_med_detaljer.append(future.result())

    with requests.Session() as s, ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_bil = {
            executor.submit(hent_kjøretøydata_svv, bil.get('identifikator'), s): bil
            for bil in biler_med_detaljer
        }
        for future in as_completed(future_to_bil):
            bil = future_to_bil[future]
            bil['svv_data'] = future.result()

    session['results'] = biler_med_detaljer
    # Merk: .show_results betyr "samme blueprint"
    return redirect(url_for('.show_results'))


@bil_import_bp.route('/results')
def show_results():
    if 'results' not in session or not session['results']:
        return render_template('results.html', biler=[], countries=[], selected_filter='alle', land_data=[])

    full_results = session['results']

    land_teller = defaultdict(int)
    for bil in full_results:
        if bil.get('svv_data'):
            land_teller[
                get_nested_safe(
                    bil,
                    ['svv_data', 'godkjenning', 'forstegangsGodkjenning', 'bruktimport', 'importland', 'landNavn'],
                    'Norge',
                )
            ] += 1
        else:
            land_teller['Ukjent'] += 1

    land_data = sorted(land_teller.items(), key=lambda item: item[1], reverse=True)

    all_countries = sorted(land_teller.keys())
    selected_filter = request.args.get('country', 'alle')

    if selected_filter == 'alle':
        filtered_results = full_results
    elif selected_filter == 'import':
        filtered_results = [
            b for b in full_results
            if b.get('svv_data') and get_nested_safe(
                b,
                ['svv_data', 'godkjenning', 'forstegangsGodkjenning', 'bruktimport', 'importland', 'landNavn'],
                'Norge',
            ) not in ['Norge']
        ]
    elif selected_filter == 'Ukjent':
        filtered_results = [b for b in full_results if not b.get('svv_data')]
    else:
        filtered_results = [
            b for b in full_results
            if b.get('svv_data') and get_nested_safe(
                b,
                ['svv_data', 'godkjenning', 'forstegangsGodkjenning', 'bruktimport', 'importland', 'landNavn'],
                'Norge',
            ) == selected_filter
        ]

    sortert_liste_biler = sorted(filtered_results, key=lambda b: b.get('pris', 0))

    return render_template(
        'results.html',
        biler=sortert_liste_biler,
        countries=all_countries,
        selected_filter=selected_filter,
        land_data=land_data,
    )

from flask import Flask, render_template, request
import requests
import json
from datetime import datetime, date

# ========================
# KONFIG
# ========================
SVV_API_KEY  = "094532a8-2343-4b4a-93f7-e284b1e0ec85"
SVV_ENDPOINT = (
    "https://www.vegvesen.no/ws/no/vegvesen/kjoretoy/felles/datautlevering/"
    "enkeltoppslag/kjoretoydata"
)

app = Flask(__name__)


# ========================
# HJELPERE
# ========================
def get_nested_safe(data, keys, default=None):
    """Trygg uthenting i nøstede dicts/lister."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int):
            try:
                current = current[key]
            except IndexError:
                return default
        else:
            return default
        if current is None:
            return default
    return current if current is not None else default


def fetch_svv_data(identifier: str):
    """Slår opp på regnr eller VIN via SVV-API-et."""
    if not identifier:
        return None, "Du må oppgi et registreringsnummer eller understellsnummer."

    ident_clean = identifier.strip().upper().replace(" ", "")
    # Svært enkel heuristikk: 7 tegn med 2 bokstaver først -> kjennemerke, ellers VIN
    if len(ident_clean) == 7 and ident_clean[:2].isalpha():
        query_param = "kjennemerke"
    else:
        query_param = "understellsnummer"

    params = {query_param: ident_clean}
    headers = {"SVV-Authorization": f"Apikey {SVV_API_KEY}"}

    try:
        r = requests.get(SVV_ENDPOINT, params=params, headers=headers, timeout=10)
    except requests.RequestException as e:
        return None, f"Feil ved kall mot SVV: {e}"

    if r.status_code != 200:
        return None, f"SVV svarte med status {r.status_code}. Sjekk reg.nr/VIN."

    data = r.json()
    lst = data.get("kjoretoydataListe") or []
    if not lst:
        return None, "Fant ingen kjøretøydata for oppgitt identifikator."

    return lst[0], None


def parse_date(date_str):
    """Parse 'YYYY-MM-DD' til date-objekt, ellers None."""
    if not date_str:
        return None
    try:
        return date.fromisoformat(date_str[:10])
    except Exception:
        return None


def flatten_svv_data(svv_data: dict) -> dict:
    """Tar rå SVV-json og lager et sett med flate, nyttige felter."""
    flat = {}

    # ---------------------------
    # Identifikasjon / kjennemerker
    # ---------------------------
    flat["svv_regnr"] = get_nested_safe(svv_data, ["kjoretoyId", "kjennemerke"])
    flat["svv_vin"] = get_nested_safe(svv_data, ["kjoretoyId", "understellsnummer"])

    kjennemerkeliste = svv_data.get("kjennemerke", [])
    personlig_plate = None
    ordinart_plate = None
    alle_plater = []

    if isinstance(kjennemerkeliste, list):
        for item in kjennemerkeliste:
            if not isinstance(item, dict):
                continue
            plate = item.get("kjennemerke")
            kategori = item.get("kjennemerkekategori")
            if plate:
                alle_plater.append(plate)
            if kategori == "KJORETOY" and ordinart_plate is None:
                ordinart_plate = plate
            if kategori == "PERSONLIG" and personlig_plate is None:
                personlig_plate = plate

    flat["svv_kjennemerke_ordinart"] = ordinart_plate or flat["svv_regnr"]
    flat["svv_kjennemerke_personlig"] = personlig_plate
    flat["svv_kjennemerke_alle"] = ", ".join(alle_plater) if alle_plater else None

    # ---------------------------
    # Generell info
    # ---------------------------
    generelt = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "generelt"],
        {},
    )

    flat["svv_merke"] = get_nested_safe(generelt, ["merke", 0, "merke"])
    flat["svv_merke_kode"] = get_nested_safe(generelt, ["merke", 0, "merkeKode"])
    flat["svv_handelsbetegnelse"] = get_nested_safe(generelt, ["handelsbetegnelse", 0])
    flat["svv_typebetegnelse"] = get_nested_safe(generelt, ["typebetegnelse"])

    kjoretoyklassifisering = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "kjoretoyklassifisering"], {}
    )
    flat["svv_kjoretoy_typebeskrivelse"] = kjoretoyklassifisering.get("beskrivelse")
    flat["svv_kjoretoy_teknisk_kode"] = get_nested_safe(
        kjoretoyklassifisering, ["tekniskKode", "kodeVerdi"]
    )
    flat["svv_kjoretoy_avgiftskode_navn"] = get_nested_safe(
        kjoretoyklassifisering, ["kjoretoyAvgiftsKode", "kodeNavn"]
    )
    flat["svv_kjoretoy_avgiftskode_verdi"] = get_nested_safe(
        kjoretoyklassifisering, ["kjoretoyAvgiftsKode", "kodeVerdi"]
    )

    # ---------------------------
    # Bruktimport / forstegangsGodkjenning
    # ---------------------------
    fg = get_nested_safe(svv_data, ["godkjenning", "forstegangsGodkjenning"], {})
    bruktimport = fg.get("bruktimport") or {}

    flat["svv_bruktimportert"] = bool(bruktimport)
    flat["svv_importland_navn"] = get_nested_safe(
        bruktimport, ["importland", "landNavn"]
    )
    flat["svv_importland_kode"] = get_nested_safe(
        bruktimport, ["importland", "landkode"]
    )
    flat["svv_import_kilometerstand"] = bruktimport.get("kilometerstand")
    flat["svv_import_tidligere_kjennemerke"] = bruktimport.get(
        "tidligereUtenlandskKjennemerke"
    )
    flat["svv_import_tidligere_vognkortnr"] = bruktimport.get(
        "tidligereUtenlandskVognkortNummer"
    )

    flat["svv_forstegang_reg_dato_utland"] = fg.get("forstegangRegistrertDato")

    fortolling = fg.get("fortollingOgMva") or {}
    flat["svv_fortolling_beskrivelse"] = fortolling.get("beskrivelse")
    flat["svv_fortolling_referanse"] = fortolling.get("fortollingsreferanse")

    # ---------------------------
    # Dimensjoner og vekter
    # ---------------------------
    dimensjoner = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "dimensjoner"], {}
    )
    flat["svv_lengde_mm"] = dimensjoner.get("lengde")
    flat["svv_bredde_mm"] = dimensjoner.get("bredde")
    flat["svv_hoyde_mm"] = dimensjoner.get("hoyde")

    vekter = get_nested_safe(
        svv_data, ["godkjenning", "tekniskGodkjenning", "tekniskeData", "vekter"], {}
    )
    flat["svv_egenvekt_kg"] = vekter.get("egenvekt")
    flat["svv_egenvekt_minimum_kg"] = vekter.get("egenvektMinimum")
    flat["svv_nyttelast_kg"] = vekter.get("nyttelast")
    flat["svv_tillatt_totalvekt_kg"] = vekter.get("tillattTotalvekt")
    flat["svv_tillatt_tilhenger_med_brems_kg"] = vekter.get(
        "tillattTilhengervektMedBrems"
    )
    flat["svv_tillatt_tilhenger_uten_brems_kg"] = vekter.get(
        "tillattTilhengervektUtenBrems"
    )

    # ---------------------------
    # Motor / drivstoff / miljø
    # ---------------------------
    motor_og_drivverk = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "motorOgDrivverk"],
        {},
    )
    motor_liste = motor_og_drivverk.get("motor") or []
    motor0 = motor_liste[0] if motor_liste else {}

    flat["svv_slagvolum_cm3"] = motor0.get("slagvolum")
    flat["svv_antall_sylindre"] = motor0.get("antallSylindre")
    flat["svv_motor_kode"] = motor0.get("motorKode")
    flat["svv_motor_arbeidsprinsipp"] = get_nested_safe(
        motor0, ["arbeidsprinsipp", "kodeBeskrivelse"]
    )

    drivstoff_liste = motor0.get("drivstoff") or []
    drivstoff0 = drivstoff_liste[0] if drivstoff_liste else {}
    flat["svv_drivstoff_navn"] = get_nested_safe(
        drivstoff0, ["drivstoffKode", "kodeBeskrivelse"]
    )
    flat["svv_maks_netto_effekt_kw"] = drivstoff0.get("maksNettoEffekt")

    miljodata = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "miljodata"],
        {},
    )
    flat["svv_euroklasse"] = get_nested_safe(
        miljodata, ["euroKlasse", "kodeBeskrivelse"]
    )

    forbruk = get_nested_safe(
        miljodata,
        ["miljoOgdrivstoffGruppe", 0, "forbrukOgUtslipp", 0],
        {},
    )
    flat["svv_forbruk_blandet_l_100km"] = forbruk.get("forbrukBlandetKjoring")
    flat["svv_co2_blandet_g_km"] = forbruk.get("co2BlandetKjoring")

    # ---------------------------
    # Persontall
    # ---------------------------
    persontall = get_nested_safe(
        svv_data,
        ["godkjenning", "tekniskGodkjenning", "tekniskeData", "persontall"],
        {},
    )
    flat["svv_sitteplasser_totalt"] = persontall.get("sitteplasserTotalt")
    flat["svv_sitteplasser_foran"] = persontall.get("sitteplasserForan")

    # ---------------------------
    # EU-kontroll
    # ---------------------------
    kontroll = svv_data.get("periodiskKjoretoyKontroll") or {}
    flat["svv_kontrollfrist"] = kontroll.get("kontrollfrist")
    flat["svv_sist_eu_godkjent"] = kontroll.get("sistGodkjent")

    # ---------------------------
    # Dekk og felg (alle kombinasjoner)
    # ---------------------------
    dekk_kombinasjoner = get_nested_safe(
        svv_data,
        [
            "godkjenning",
            "tekniskGodkjenning",
            "tekniskeData",
            "dekkOgFelg",
            "akselDekkOgFelgKombinasjon",
        ],
        [],
    )

    standard_foran = None
    standard_bak = None
    alle_kombinasjoner_tekst = []
    unike_dekkdimensjoner = set()

    if isinstance(dekk_kombinasjoner, list):
        for idx, komb in enumerate(dekk_kombinasjoner, start=1):
            aksel_liste = get_nested_safe(komb, ["akselDekkOgFelg"], [])
            aksel_tekster = []

            if not isinstance(aksel_liste, list):
                continue

            for aksel in aksel_liste:
                if not isinstance(aksel, dict):
                    continue

                aksel_id = aksel.get("akselId")
                dekkdim = aksel.get("dekkdimensjon")
                felgdim = aksel.get("felgdimensjon")
                hast = aksel.get("hastighetskodeDekk")
                belast = aksel.get("belastningskodeDekk")
                innpress = aksel.get("innpress")
                tvilling = aksel.get("tvilling")

                aksel_tekst = f"aksel {aksel_id}: {dekkdim} {felgdim} {belast}{hast}, ET{innpress}, tvilling={tvilling}"
                aksel_tekster.append(aksel_tekst)

                if dekkdim:
                    unike_dekkdimensjoner.add(dekkdim)

                if idx == 1:
                    combo_str = f"{dekkdim} {felgdim} {belast}{hast}, ET{innpress}"
                    if aksel_id == 1 and standard_foran is None:
                        standard_foran = combo_str
                    if aksel_id == 2 and standard_bak is None:
                        standard_bak = combo_str

            if aksel_tekster:
                alle_kombinasjoner_tekst.append(
                    f"Kombinasjon {idx}: " + " | ".join(aksel_tekster)
                )

    flat["svv_dekk_standard_foran"] = standard_foran
    flat["svv_dekk_standard_bak"] = standard_bak
    flat["svv_dekkdimensjoner_unike"] = (
        ", ".join(sorted(unike_dekkdimensjoner)) if unike_dekkdimensjoner else None
    )
    flat["svv_dekk_alle_kombinasjoner"] = (
        "; ".join(alle_kombinasjoner_tekst) if alle_kombinasjoner_tekst else None
    )

    return flat


def compute_eu_status(kontrollfrist_str: str):
    """Returnerer (status, dager_igjen) for EU-kontroll."""
    frist = parse_date(kontrollfrist_str)
    if not frist:
        return "ukjent", None

    today = date.today()
    diff = (frist - today).days

    if diff < 0:
        return "utløpt", diff
    elif diff <= 183:  # ca 6 mnd
        return "snart", diff
    else:
        return "ok", diff


# ========================
# FLASK-RUTE
# ========================
@app.route("/", methods=["GET", "POST"])
def index():
    svv_raw = None
    flat = None
    error = None
    eu_status = None
    eu_dager_igjen = None

    if request.method == "POST":
        ident = request.form.get("identifier", "").strip()
        svv_raw, error = fetch_svv_data(ident)
        if svv_raw and not error:
            flat = flatten_svv_data(svv_raw)
            eu_status, eu_dager_igjen = compute_eu_status(
                flat.get("svv_kontrollfrist")
            )

    # Pretty JSON for visning
    pretty_json = json.dumps(svv_raw, indent=2, ensure_ascii=False) if svv_raw else None

    return render_template(
        "index.html",
        flat=flat,
        raw_json=pretty_json,
        error=error,
        eu_status=eu_status,
        eu_dager_igjen=eu_dager_igjen,
    )


if __name__ == "__main__":
    app.run(debug=True)

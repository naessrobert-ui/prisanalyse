# bil_routes.py
import json
from datetime import datetime, timedelta, date
from rekordrask_parquet import bygg_visning_for_solgte_fra_parquet

import boto3
import awswrangler as wr
import pandas as pd
from flask import Blueprint, render_template, jsonify, request

from config import (
    AWS_KEY,
    AWS_SECRET,
    AWS_REGION,
    S3_BUCKET_NAME,
    ATHENA_DATABASE,
    DEFAULT_STARTDATE,
    ATHENA_TABLE,          # 👈 legg til denne
)


FINN_BASE_URL = "https://www.finn.no/mobility/item/"

# NYTT: gjenbruk logikken fra svv_app.pyapp.from svv_app import fetch_svv_data, flatten_svv_data, compute_eu_status

bil_bp = Blueprint('bil', __name__, url_prefix='/bil')
from svv_app import fetch_svv_data, flatten_svv_data, compute_eu_status


# ------------------ Felles hjelp ------------------

def _get_metadata():
    """Henter metadata for produsenter/modeller mv. fra S3."""
    try:
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET
        )
        meta_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key='calc/metadata.json')
        metadata = json.loads(meta_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"ADVARSEL: Kunne ikke laste metadata for bil. Feil: {e}")
        metadata = {}
    return metadata

def _hent_bil_data_fra_athena(filters: dict) -> pd.DataFrame:
    """Kjører Athena-spørring mot database_biler_parquet basert på filtrene."""
    my_session = boto3.Session(
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

    # --------- Startdato (tåler dd.mm.yyyy og yyyy-mm-dd) ---------
    start_str = filters.get("startdato")
    startdato = DEFAULT_STARTDATE
    if start_str:
        parsed = None
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                parsed = datetime.strptime(start_str, fmt).date()
                break
            except ValueError:
                continue
        if parsed:
            startdato = parsed

    # Dato er timestamp i Athena – cast til date
    dato_expr = "TRY_CAST(dato AS date)"

    where_clauses: list[str] = [
        f"{dato_expr} IS NOT NULL",
        f"{dato_expr} >= DATE '{startdato.isoformat()}'",
    ]

    # --------- Produsent / modell ---------
    prod = filters.get("produsent")
    if prod:
        safe_prod = prod.replace("'", "''")
        where_clauses.append("produsent = '" + safe_prod + "'")

    mod = filters.get("modell")
    if mod:
        safe_mod = mod.replace("'", "''")
        where_clauses.append("modell = '" + safe_mod + "'")

    # --------- Tekstsøk ---------
    modell_sok = filters.get("modell_sok")
    if modell_sok:
        safe = modell_sok.lower().replace("'", "''")
        where_clauses.append("LOWER(overskrift) LIKE '%" + safe + "%'")

    seller_sok = filters.get("seller_sok")
    if seller_sok:
        safe = seller_sok.lower().replace("'", "''")
        where_clauses.append("LOWER(selger) LIKE '%" + safe + "%'")

    # --------- Range / pris / km / år ---------
    if filters.get("range_min"):
        where_clauses.append("rekkevidde_str >= " + str(int(filters["range_min"])))
    if filters.get("range_max"):
        where_clauses.append("rekkevidde_str <= " + str(int(filters["range_max"])))

    if filters.get("pris_min"):
        where_clauses.append("pris_num >= " + str(int(filters["pris_min"])))
    if filters.get("pris_max"):
        where_clauses.append("pris_num <= " + str(int(filters["pris_max"])))

    if filters.get("km_max"):
        where_clauses.append('"kjørelengde" <= ' + str(int(filters["km_max"])))

    # **Nytt: både år fra og år til**
    if filters.get("year_min"):
        where_clauses.append('"årstall" >= ' + str(int(filters["year_min"])))
    if filters.get("year_max"):
        where_clauses.append('"årstall" <= ' + str(int(filters["year_max"])))

    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT
            finnkode,
            {dato_expr} AS dato,
            produsent,
            modell,
            overskrift,
            "årstall"     AS "årstall",
            "kjørelengde" AS "kjørelengde",
            drivstoff,
            hjuldrift,
            rekkevidde_str,
            selger,
            pris_num
        FROM {ATHENA_TABLE}
        WHERE {where_sql}
    """

    print("----- ATHENA SQL -----")
    print(query)

    df = wr.athena.read_sql_query(
        sql=query,
        database=ATHENA_DATABASE,
        s3_output=f"s3://{S3_BUCKET_NAME}/athena-results/",
        boto3_session=my_session
    )

    print("----- Kolonner tilgjengelig i DataFrame: -----")
    print(df.columns.tolist())
    print("----- De 5 første radene med data: -----")
    print(df.head())

    if df.empty:
        return df

    df.columns = [c.lower() for c in df.columns]
    return df


def _bygg_historikk(df: pd.DataFrame) -> pd.DataFrame:
    """Bygger historikk_df med dager og prisfall per Finn-kode."""
    historikk_df = df.sort_values('dato').groupby('finnkode').agg(
        produsent=('produsent', 'last'),
        modell=('modell', 'last'),
        overskrift=('overskrift', 'last'),
        årstall=('årstall', 'last'),
        kjørelengde=('kjørelengde', 'last'),
        drivstoff=('drivstoff', 'last'),
        hjuldrift=('hjuldrift', 'last'),
        rekkevidde=('rekkevidde_str', 'last'),
        selger=('selger', 'last'),
        dato_start=('dato', 'first'),
        dato_end=('dato', 'last'),
        pris_start=('pris_num', 'first'),
        pris_last=('pris_num', lambda x: x[x > 0].iloc[-1] if not x[x > 0].empty else None),
    ).reset_index()

    historikk_df['dager'] = (
        pd.to_datetime(historikk_df['dato_end']) - pd.to_datetime(historikk_df['dato_start'])
    ).dt.days
    historikk_df['prisfall'] = historikk_df['pris_last'] - historikk_df['pris_start']

    # Finn-URL (klikkbar lenke i tabellen)
    historikk_df['finn_url'] = FINN_BASE_URL + historikk_df['finnkode'].astype(str)

    # Sorter default på pris (lavest først)
    historikk_df = historikk_df.sort_values('pris_last', ascending=True)

    return historikk_df


# ------------------ Ruter ------------------


@bil_bp.route('/')
def bil_landing():
    """Underside for bilanalyse – hub med flere bil-apper."""
    return render_template('bil_landing.html')


# ---- 1. Dette ble bilene solgt for ----

@bil_bp.route('/solgt')
def bil_solgt_analyse_side():
    metadata = _get_metadata()

    return render_template(
        'bil_analyse_template.html',
        tittel="Dette ble bilene solgt for",
        data_url="/bil/solgt/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=metadata.get ('models_by_prod', {}),
        drivstoff_opts=metadata.get('drivstoff_opts', []),
        hjuldrift_opts=metadata.get('hjuldrift_opts', []),
        year_min=metadata.get('year_min', 2000),
        year_max=metadata.get('year_max', pd.Timestamp.now().year),
        km_min=metadata.get('km_min', 0),
        km_max=metadata.get('km_max', 200000),
    )


@bil_bp.route('/solgt/data', methods=['POST'])
def get_bil_solgt_data():
    try:
        filters = request.get_json().get('filters', {})
        df = _hent_bil_data_fra_athena(filters)
        if df.empty:
            return jsonify({'historikk': [], 'daily_stats': [], 'kpis': {}})

        # Drivstoff / hjuldrift / år / km filter gjøres trygt i pandas
        if filters.get('drivstoff'):
            df = df[df['drivstoff'].isin(filters['drivstoff'])]
        if filters.get('hjuldrift'):
            df = df[df['hjuldrift'].isin(filters['hjuldrift'])]
        if filters.get('year_min'):
            df = df[df['årstall'] >= int(filters['year_min'])]
        if filters.get('year_max'):
            df = df[df['årstall'] <= int(filters['year_max'])]
        if filters.get('km_min'):
            df = df[df['kjørelengde'] >= int(filters['km_min'])]
        if filters.get('km_max'):
            df = df[df['kjørelengde'] <= int(filters['km_max'])]

        if df.empty:
            return jsonify({'historikk': [], 'daily_stats': [], 'kpis': {}})

        historikk_df = _bygg_historikk(df)

        usolgte_biler = historikk_df[historikk_df['pris_last'] > 0]
        kpis = {}
        if not usolgte_biler.empty:
            kpis = {
                'avg_dager': int(usolgte_biler['dager'].mean()),
                'median_dager': int(usolgte_biler['dager'].median()),
                'avg_pris': int(usolgte_biler['pris_last'].mean()),
                'median_pris': int(usolgte_biler['pris_last'].median()),
                'laveste_pris': int(usolgte_biler['pris_last'].min()),
            }

        daily_stats_df = df.groupby('dato').agg(
            Antall_Solgt=('pris_num', lambda x: (x == 0).sum()),
            Median_Pris_Usolgt=('pris_num', lambda x: x[x > 0].median()),
        ).reset_index()
        daily_stats_df['Dato'] = pd.to_datetime(daily_stats_df['dato']).dt.strftime('%Y-%m-%d')

        daily_stats = json.loads(daily_stats_df.to_json(orient='records')) if not daily_stats_df.empty else []
        historikk_df = historikk_df.where(pd.notna(historikk_df), None)
        historikk = json.loads(historikk_df.to_json(orient='records')) if not historikk_df.empty else []

        return jsonify({'historikk': historikk, 'daily_stats': daily_stats, 'kpis': kpis})

    except Exception as e:
        print(f"Feil i /bil/solgt/data: {e}")
        return jsonify({"error": str(e)}), 500


# ---- 2. Biler solgt rekordraskt ----

@bil_bp.route('/rekordrask')
def bil_rekordrask_side():
    metadata = _get_metadata()
    return render_template(
        'bil_rekordrask.html',
        tittel="Biler solgt rekordraskt",
        data_url="/bil/rekordrask/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=json.dumps(metadata.get('models_by_prod', {})),
        default_startdate=(date.today() - timedelta(days=3)).isoformat(),
    )


@bil_bp.route('/rekordrask/data', methods=['POST'])
def get_bil_rekordrask_data():
    """
    Henter 'rekordraskt solgte' biler fra Parquet-filen i S3,
    filtrerer på produsent/modell/pris/km/år osv. og returnerer JSON
    i formatet som bil_rekordrask.html forventer.
    """
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        # ---- Startdato (historikk fra) ----
        start_str = filters.get('startdato')
        if start_str:
            startdato = datetime.strptime(start_str, "%Y-%m-%d").date()
        else:
            # default: 3 siste dager
            startdato = date.today() - timedelta(days=3)

        # Bygg visning fra Parquet
        vis_solgte = bygg_visning_for_solgte_fra_parquet(startdato)

        if vis_solgte.empty:
            return jsonify({'rows': [], 'kpis': {}})

        # ---- Pandas-filtre tilsvarende UI ----
        # Produsent / Merke
        prod = filters.get('produsent')
        if prod:
            vis_solgte = vis_solgte[vis_solgte['Merke'] == prod]

        # Modell
        mod = filters.get('modell')
        if mod:
            vis_solgte = vis_solgte[vis_solgte['Modell'] == mod]

        # Pris
        if filters.get('pris_min'):
            vis_solgte = vis_solgte[
                vis_solgte['Pris'].fillna(0) >= int(filters['pris_min'])
            ]
        if filters.get('pris_max'):
            vis_solgte = vis_solgte[
                vis_solgte['Pris'].fillna(0) <= int(filters['pris_max'])
            ]

        # Km maks
        if filters.get('km_max'):
            vis_solgte = vis_solgte[
                vis_solgte['Km'].fillna(10**9) <= int(filters['km_max'])
            ]

        # Min år
        if filters.get('year_min'):
            vis_solgte = vis_solgte[
                vis_solgte['Årsmodell'].fillna(0) >= int(filters['year_min'])
            ]

        # Maks dager til salg (UI: max_dager, backend: timer_til_salg)
        if filters.get('max_timer') or filters.get('max_dager'):
            # I HTML-en heter det max_dager, men vi brukte max_timer i backend før
            val = filters.get('max_timer') or filters.get('max_dager')
            max_dager = int(val)
            max_timer = max_dager * 24
            vis_solgte = vis_solgte[
                vis_solgte['timer_til_salg'] <= max_timer
            ]

        if vis_solgte.empty:
            return jsonify({'rows': [], 'kpis': {}})

        # ---- KPIs ----
        min_timer = float(vis_solgte['timer_til_salg'].min())
        median_timer = float(vis_solgte['timer_til_salg'].median())
        avg_timer = float(vis_solgte['timer_til_salg'].mean())

        kpis = {
            'min_timer': int(round(min_timer)),
            'median_timer': median_timer,
            'avg_timer': avg_timer,
        }

        # Begrens antall rader litt (f.eks. 500) for frontend
        vis_solgte = vis_solgte.sort_values('timer_til_salg', ascending=True).head(500)

        # Sørg for at NaN -> None før JSON
        vis_solgte = vis_solgte.where(pd.notna(vis_solgte), None)
        rows = json.loads(vis_solgte.to_json(orient='records'))

        return jsonify({'rows': rows, 'kpis': kpis})

    except Exception as e:
        print(f"Feil i /bil/rekordrask/data: {e}")
        return jsonify({"error": str(e)}), 500


# ---- 3. Import – enkel placeholder inntil videre ----

@bil_bp.route('/import')
def bil_import_placeholder():
    return """
    <html><body style="font-family: sans-serif; background:#020617; color:#e5e7eb; text-align:center; padding:40px;">
      <h1>Import – hvor kommer bilene fra?</h1>
      <p>Modulen er ikke koblet til ennå. Vi kommer til å vise opprinnelsesland og importstrømmer basert på dine data.</p>
      <p style="margin-top:20px;"><a href="/bil" style="color:#60a5fa;">← Tilbake til bilanalyse</a></p>
    </body></html>
    """


# ---- 4. SVV-oppslag integrert på /bil/svv ----

@bil_bp.route('/svv', methods=['GET', 'POST'])
def bil_svv_side():
    """
    SVV-oppslag integrert i hoved-appen på /bil/svv.
    Gjenbruker logikken fra svv_app.py (fetch_svv_data, flatten_svv_data, compute_eu_status)
    og rendrer template 'bil_svv.html'.
    """
    svv_raw = None
    flat = None
    error = None
    eu_status = None
    eu_dager_igjen = None

    if request.method == "POST":
        ident = (request.form.get("identifier") or "").strip()
        if not ident:
            error = "Du må oppgi et registreringsnummer eller understellsnummer."
        else:
            svv_raw, error = fetch_svv_data(ident)
            if svv_raw and not error:
                flat = flatten_svv_data(svv_raw)
                eu_status, eu_dager_igjen = compute_eu_status(
                    flat.get("svv_kontrollfrist")
                )

    pretty_json = json.dumps(svv_raw, indent=2, ensure_ascii=False) if svv_raw else None

    return render_template(
        "bil_svv.html",
        flat=flat,
        raw_json=pretty_json,
        error=error,
        eu_status=eu_status,
        eu_dager_igjen=eu_dager_igjen,
    )



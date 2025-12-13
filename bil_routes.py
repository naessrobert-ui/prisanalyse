# bil_routes.py
import json
from datetime import datetime, timedelta, date
import boto3
import pandas as pd
import io
import numpy as np
from flask import Blueprint, render_template, jsonify, request
import traceback # Importert for bedre feilhåndtering

from config import (
    AWS_KEY,
    AWS_SECRET,
    AWS_REGION,
    S3_BUCKET_NAME,
    DEFAULT_STARTDATE
)

from rekordrask_parquet import bygg_visning_for_solgte_fra_parquet

bil_bp = Blueprint('bil', __name__, url_prefix='/bil')
from svv_app import fetch_svv_data, flatten_svv_data, compute_eu_status

FINN_BASE_URL = "https://www.finn.no/mobility/item/"
PARQUET_FILE_KEY = "calc/bil/database_biler.parquet"
METADATA_KEY = "calc/metadata.json"


# ------------------ Felles hjelp ------------------

def _get_s3_client():
    return boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )


def _get_metadata():
    try:
        s3 = _get_s3_client()
        meta_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=METADATA_KEY)
        metadata = json.loads(meta_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"ADVARSEL: Kunne ikke laste metadata. Feil: {e}")
        metadata = {}
    return metadata


def _last_inn_hele_databasen() -> pd.DataFrame:
    try:
        s3 = _get_s3_client()
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=PARQUET_FILE_KEY)
        buffer = io.BytesIO(obj['Body'].read())
        df = pd.read_parquet(buffer)
        return df
    except Exception as e:
        print(f"Feil ved lesing av Parquet: {e}")
        return pd.DataFrame()


def _filtrer_data(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Ryddet opp:
    - Dato-filter bruker kun Dato_ny (sist observert for salg)
    - Pris-filter bruker kun Pris_ny (sist observert pris)
    """
    if df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}

    def get_col(candidates):
        for cand in candidates:
            if cand.lower() in cols:
                return cols[cand.lower()]
        return None

    col_prod = get_col(['Produsent', 'produsent'])
    col_mod = get_col(['Modell', 'modell'])
    col_over = get_col(['Overskrift', 'overskrift', 'info'])
    col_selger = get_col(['Selger', 'selger'])

    # Kun to priser i datasettet
    col_pris_ny = get_col(['Pris_ny', 'pris_ny'])

    # Kun to datoer i datasettet
    col_sist_obs = get_col(['Dato_ny', 'dato_ny'])

    col_km = get_col(['kjørelengde', 'km'])
    col_aar = get_col(['årstall', 'year'])
    col_rekk = get_col(['rekkevidde_str', 'rekkevidde'])
    col_driv = get_col(['drivstoff'])
    col_hjul = get_col(['hjuldrift'])

    # --- Filtrering ---

    # Dato-filter: bruk Dato_ny
    start_str = filters.get("startdato")
    if start_str and col_sist_obs:
        startdato = pd.to_datetime(start_str, errors='coerce')
        if pd.notna(startdato):
            df[col_sist_obs] = pd.to_datetime(df[col_sist_obs], errors='coerce')
            df = df[df[col_sist_obs] >= startdato]

    # Tekst
    if filters.get("produsent") and col_prod:
        df = df[df[col_prod] == filters["produsent"]]

    if filters.get("modell") and col_mod:
        df = df[df[col_mod] == filters["modell"]]

    if filters.get("modell_sok") and col_over:
        sok = filters["modell_sok"].lower()
        df = df[df[col_over].astype(str).str.lower().str.contains(sok, na=False)]

    if filters.get("seller_sok") and col_selger:
        sok = filters["seller_sok"].lower()
        df = df[df[col_selger].astype(str).str.lower().str.contains(sok, na=False)]

    # Tall
    if col_pris_ny:
        df[col_pris_ny] = pd.to_numeric(df[col_pris_ny], errors='coerce')
        if filters.get("pris_min"):
            df = df[df[col_pris_ny] >= int(filters["pris_min"])]
        if filters.get("pris_max"):
            df = df[df[col_pris_ny] <= int(filters["pris_max"])]

    if col_km and filters.get("km_max"):
        df[col_km] = pd.to_numeric(df[col_km], errors='coerce')
        df = df[df[col_km] <= int(filters["km_max"])]

    if col_aar:
        df[col_aar] = pd.to_numeric(df[col_aar], errors='coerce')
        if filters.get("year_min"):
            df = df[df[col_aar] >= int(filters["year_min"])]
        if filters.get("year_max"):
            df = df[df[col_aar] <= int(filters["year_max"])]

    if col_rekk:
        df[col_rekk] = pd.to_numeric(df[col_rekk], errors='coerce')
        if filters.get("range_min"):
            df = df[df[col_rekk] >= int(filters["range_min"])]
        if filters.get("range_max"):
            df = df[df[col_rekk] <= int(filters["range_max"])]

    if col_driv and filters.get("drivstoff"):
        d_list = filters["drivstoff"]
        if isinstance(d_list, list):
            df = df[df[col_driv].isin(d_list)]

    if col_hjul and filters.get("hjuldrift"):
        h_list = filters["hjuldrift"]
        if isinstance(h_list, list):
            df = df[df[col_hjul].isin(h_list)]

    return df


# ------------------ Ruter ------------------

@bil_bp.route('/')
def bil_landing():
    return render_template('bil_landing.html')


@bil_bp.route('/solgt')
def bil_solgt_analyse_side():
    metadata = _get_metadata()
    return render_template(
        'bil_analyse_template.html',
        tittel="Dette ble bilene solgt for",
        data_url="/bil/solgt/data",
        produsenter=metadata.get('produsenter', []),
        models_by_prod=metadata.get('models_by_prod', {}),
        drivstoff_opts=metadata.get('drivstoff_opts', []),
        hjuldrift_opts=metadata.get('hjuldrift_opts', []),
        year_min=metadata.get('year_min', 2000),
        year_max=metadata.get('year_max', datetime.now().year),
        km_min=metadata.get('km_min', 0),
        km_max=metadata.get('km_max', 200000),
    )

# --- HER ER HOVEDENDRINGEN ---
@bil_bp.route('/solgt/data', methods=['POST'])
def get_bil_solgt_data():
    try:
        # Definer maks antall rader vi tillater å behandle
        MAX_ROWS_LIMIT = 800

        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}

        # Dette laster hele databasen inn i minnet (flaskehalsen på Render)
        df = _last_inn_hele_databasen()
        if df.empty:
            # Returnerer status 'ok' men tomme lister
            return jsonify({'status': 'ok', 'historikk': [], 'daily_stats': [], 'kpis': {}})

        # Filtrerer dataene i minnet
        df_filtered = _filtrer_data(df, filters)

        # --- NY SJEKK: Stopper hvis det er for mange treff ---
        antall_treff = len(df_filtered)

        if antall_treff > MAX_ROWS_LIMIT:
            print(f"Advarsel: Søk ga {antall_treff} treff. Stopper behandling pga grense på {MAX_ROWS_LIMIT}.")
            # Returnerer en advarsel til frontend, og IKKE dataene.
            return jsonify({
                'status': 'warning',
                'message': f'Søket ga for mange treff ({antall_treff}). Visning er begrenset til søk med under {MAX_ROWS_LIMIT} resultater for å sikre ytelse.',
                'count': antall_treff,
                # Sender tomme datastrukturer for å unngå feil i frontend
                'historikk': [],
                'daily_stats': [],
                'kpis': {}
            })
        # --- SLUTT NY SJEKK ---


        if df_filtered.empty:
             # Returnerer status 'ok' men tomme lister
            return jsonify({'status': 'ok', 'historikk': [], 'daily_stats': [], 'kpis': {}})

        # --- Start tung dataprosessering (kjøres kun hvis under grensen) ---
        cols = {c.lower(): c for c in df_filtered.columns}

        def find_col(options):
            for key in options:
                if key.lower() in cols:
                    return cols[key.lower()]
            return None

        # --- Kun to prisfelt ---
        c_pris_start = find_col(['Pris', 'pris'])
        c_pris_ny = find_col(['Pris_ny', 'pris_ny'])

        # --- Kun to datofelt ---
        c_dato_start = find_col(['Dato', 'dato'])
        c_dato_ny = find_col(['Dato_ny', 'dato_ny'])

        # Andre felt
        c_finnkode = find_col(['FinnKode', 'finnkode'])
        c_overskrift = find_col(['Overskrift', 'overskrift', 'info'])
        c_solgt = find_col(['Solgt', 'solgt'])

        # Sikre numeriske verdier
        if c_pris_start and c_pris_start in df_filtered.columns:
            df_filtered[c_pris_start] = pd.to_numeric(df_filtered[c_pris_start], errors='coerce').fillna(0)
        else:
            c_pris_start = 'Pris'
            df_filtered[c_pris_start] = 0

        if c_pris_ny and c_pris_ny in df_filtered.columns:
            df_filtered[c_pris_ny] = pd.to_numeric(df_filtered[c_pris_ny], errors='coerce').fillna(0)
        else:
            c_pris_ny = 'Pris_ny'
            df_filtered[c_pris_ny] = 0

        # Sikre datoer
        if c_dato_start and c_dato_start in df_filtered.columns:
            df_filtered[c_dato_start] = pd.to_datetime(df_filtered[c_dato_start], errors='coerce')
        if c_dato_ny and c_dato_ny in df_filtered.columns:
            df_filtered[c_dato_ny] = pd.to_datetime(df_filtered[c_dato_ny], errors='coerce')

        # Dager: Dato_ny - Dato
        if c_dato_start in df_filtered.columns and c_dato_ny in df_filtered.columns:
            df_filtered['dager'] = (df_filtered[c_dato_ny] - df_filtered[c_dato_start]).dt.days
            df_filtered['dager'] = df_filtered['dager'].fillna(0).astype(int).clip(lower=0)
        else:
            df_filtered['dager'] = 0

        # Prisendring: Pris_ny - Pris (negativt = kutt)
        df_filtered['pris_endring'] = df_filtered[c_pris_ny] - df_filtered[c_pris_start]

        # Finn-link
        if c_finnkode and c_finnkode in df_filtered.columns:
            df_filtered[c_finnkode] = (
                df_filtered[c_finnkode]
                .astype(str)
                .str.replace(r'\.0$', '', regex=True)
            )
            df_filtered['finn_url'] = FINN_BASE_URL + df_filtered[c_finnkode]
        else:
            df_filtered['finn_url'] = None

        # --- Output mapping (frontend-navn) ---
        rename_map = {
            'Produsent': 'produsent',
            'Modell': 'modell',
            'årstall': 'årstall',
            'kjørelengde': 'kjørelengde',
            'drivstoff': 'drivstoff',
            'hjuldrift': 'hjuldrift',
            'rekkevidde_str': 'rekkevidde',
            'selger': 'selger',

            # Info
            c_overskrift: 'overskrift' if c_overskrift else None,

            # Datoer
            c_dato_start: 'dato_start' if c_dato_start else None,   # = Dato
            c_dato_ny: 'dato_end' if c_dato_ny else None,           # = Dato_ny (sist observert)

            # Priser
            c_pris_start: 'pris_start' if c_pris_start else None,   # = Pris
            c_pris_ny: 'pris_ny' if c_pris_ny else None,            # = Pris_ny

            # Kode/solgt
            c_finnkode: 'finnkode' if c_finnkode else None,
            c_solgt: 'solgt' if c_solgt else None
        }

        rename_map = {
            k: v for k, v in rename_map.items()
            if k is not None and v is not None and k in df_filtered.columns
        }

        output_df = df_filtered.rename(columns=rename_map)

        # Bakoverkomp: pris_last peker på siste pris
        if 'pris_ny' in output_df.columns and 'pris_last' not in output_df.columns:
            output_df['pris_last'] = output_df['pris_ny']

        # Sikre pris_endring også i output (den finnes i df_filtered, men beholdes uansett)
        if 'pris_endring' not in output_df.columns and 'pris_start' in output_df.columns and 'pris_ny' in output_df.columns:
            output_df['pris_endring'] = (
                pd.to_numeric(output_df['pris_ny'], errors='coerce').fillna(0)
                - pd.to_numeric(output_df['pris_start'], errors='coerce').fillna(0)
            )

        # Sørg for datetime i output for grouping
        if 'dato_end' in output_df.columns:
            output_df['dato_end'] = pd.to_datetime(output_df['dato_end'], errors='coerce')

        # Sorter på siste pris (pris_ny)
        if 'pris_ny' in output_df.columns:
            output_df = output_df.sort_values('pris_ny', ascending=True)

        # --- KPIer og grafer ---
        kpis = {}
        daily_stats = []

        # Bruk solgt-flagget hvis det finnes, ellers fallback
        if 'pris_ny' in output_df.columns:
            if 'solgt' in output_df.columns:
                solgte = output_df[output_df['solgt'].astype(bool)]
            else:
                solgte = output_df[output_df['pris_ny'] > 1000]

            if not solgte.empty:
                kpis = {
                    'avg_dager': int(solgte['dager'].mean()),
                    'median_dager': int(solgte['dager'].median()),
                    'avg_pris': int(solgte['pris_ny'].mean()),
                    'median_pris': int(solgte['pris_ny'].median()),
                    'laveste_pris': int(solgte['pris_ny'].min()),
                    'antall': len(solgte)
                }

                if 'dato_end' in solgte.columns:
                    solgte = solgte.copy()
                    solgte['dato_end'] = pd.to_datetime(solgte['dato_end'], errors='coerce')
                    solgte = solgte.dropna(subset=['dato_end'])

                    daily_stats_df = solgte.groupby(solgte['dato_end'].dt.date).agg(
                        Antall_Solgt=('pris_ny', 'count'),
                        Median_Pris=('pris_ny', 'median')
                    ).reset_index()

                    # Frontend-komp: beholder også gamle nøkkelnavn
                    daily_stats_df.rename(columns={'dato_end': 'Dato'}, inplace=True)
                    daily_stats_df['Dato'] = pd.to_datetime(daily_stats_df['Dato']).dt.strftime('%Y-%m-%d')
                    daily_stats_df['Median_Pris_Usolgt'] = daily_stats_df['Median_Pris']

                    daily_stats = json.loads(daily_stats_df.to_json(orient='records'))

        # JSON Export
        output_df = output_df.where(pd.notna(output_df), None)

        # (Den gamle begrensningen på 2000 er nå overflødig og kan fjernes,
        # siden vi stopper mye tidligere hvis det er over 250)
        # if len(output_df) > 2000:
        #     output_df = output_df.head(2000)

        historikk = json.loads(output_df.to_json(orient='records', date_format='iso'))

        # Returnerer suksess med status 'ok'
        return jsonify({'status': 'ok', 'historikk': historikk, 'daily_stats': daily_stats, 'kpis': kpis})

    except Exception as e:
        print(f"Feil i /bil/solgt/data: {e}")
        traceback.print_exc()
        # Returnerer feil med status 'error'
        return jsonify({"status": "error", "error": str(e), "message": "En uventet feil oppstod på serveren."}), 500


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
    try:
        payload = request.get_json() or {}
        filters = payload.get('filters', {}) or {}
        start_str = filters.get('startdato')
        if start_str:
            startdato = datetime.strptime(start_str, "%Y-%m-%d").date()
        else:
            startdato = date.today() - timedelta(days=3)

        vis_solgte = bygg_visning_for_solgte_fra_parquet(startdato)

        if vis_solgte.empty:
            # Oppdatert med status
            return jsonify({'status': 'ok', 'rows': [], 'kpis': {}})

        vis_solgte = vis_solgte.where(pd.notna(vis_solgte), None)
        rows = json.loads(vis_solgte.to_json(orient='records'))
        # Oppdatert med status
        return jsonify({'status': 'ok', 'rows': rows, 'kpis': {}})

    except Exception as e:
        print(f"Feil i /bil/rekordrask/data: {e}")
        # Oppdatert med status
        return jsonify({"status": "error", "error": str(e)}), 500


@bil_bp.route('/svv', methods=['GET', 'POST'])
def bil_svv_side():
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
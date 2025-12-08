# bil_routes.py
import json
from datetime import datetime, timedelta, date
import boto3
import pandas as pd
import io
import numpy as np
from flask import Blueprint, render_template, jsonify, request

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
    if df.empty: return df

    # Finn riktige kolonnenavn (case-insensitive)
    cols = {c.lower(): c for c in df.columns}

    # Hjelpefunksjon for å finne kolonne
    def get_col(candidates):
        for cand in candidates:
            if cand.lower() in cols: return cols[cand.lower()]
        return None

    col_prod = get_col(['Produsent', 'produsent'])
    col_mod = get_col(['Modell', 'modell'])
    col_over = get_col(['Overskrift', 'overskrift', 'info'])
    col_selger = get_col(['selger', 'Selger'])
    col_pris_ny = get_col(['Pris_Ny', 'Pris_num', 'pris_last'])
    col_slutt = get_col(['slutt_dato', 'dato_end'])
    col_km = get_col(['kjørelengde', 'km'])
    col_aar = get_col(['årstall', 'year'])
    col_rekk = get_col(['rekkevidde_str', 'rekkevidde'])
    col_driv = get_col(['drivstoff'])
    col_hjul = get_col(['hjuldrift'])

    # --- Filtrering ---

    # Dato
    start_str = filters.get("startdato")
    if start_str and col_slutt:
        startdato = pd.to_datetime(start_str)
        df = df[df[col_slutt] >= startdato]

    # Tekst
    if filters.get("produsent") and col_prod:
        df = df[df[col_prod] == filters["produsent"]]

    if filters.get("modell") and col_mod:
        df = df[df[col_mod] == filters["modell"]]

    if filters.get("modell_sok") and col_over:
        sok = filters["modell_sok"].lower()
        df = df[df[col_over].str.lower().str.contains(sok, na=False)]

    if filters.get("seller_sok") and col_selger:
        sok = filters["seller_sok"].lower()
        df = df[df[col_selger].str.lower().str.contains(sok, na=False)]

    # Tall
    if col_pris_ny:
        if filters.get("pris_min"): df = df[df[col_pris_ny] >= int(filters["pris_min"])]
        if filters.get("pris_max"): df = df[df[col_pris_ny] <= int(filters["pris_max"])]

    if col_km and filters.get("km_max"):
        df = df[df[col_km] <= int(filters["km_max"])]

    if col_aar:
        if filters.get("year_min"): df = df[df[col_aar] >= int(filters["year_min"])]
        if filters.get("year_max"): df = df[df[col_aar] <= int(filters["year_max"])]

    if col_rekk:
        if filters.get("range_min"): df = df[df[col_rekk] >= int(filters["range_min"])]
        if filters.get("range_max"): df = df[df[col_rekk] <= int(filters["range_max"])]

    if col_driv and filters.get("drivstoff"):
        d_list = filters["drivstoff"]
        if isinstance(d_list, list): df = df[df[col_driv].isin(d_list)]

    if col_hjul and filters.get("hjuldrift"):
        h_list = filters["hjuldrift"]
        if isinstance(h_list, list): df = df[df[col_hjul].isin(h_list)]

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


@bil_bp.route('/solgt/data', methods=['POST'])
def get_bil_solgt_data():
    try:
        filters = request.get_json().get('filters', {})
        df = _last_inn_hele_databasen()

        if df.empty:
            return jsonify({'historikk': [], 'daily_stats': [], 'kpis': {}})

        # Filtrer først for å redusere datamengden
        df_filtered = _filtrer_data(df, filters)

        if df_filtered.empty:
            return jsonify({'historikk': [], 'daily_stats': [], 'kpis': {}})

        # --- KOLONNE-MAPPING (Sikrer at vi finner dataene) ---
        cols = {c.lower(): c for c in df_filtered.columns}

        c_pris_start = cols.get('pris', 'Pris')
        c_pris_slutt = cols.get('pris_ny', 'Pris_Ny')  # Dette er "Siste pris"
        c_dato_start = cols.get('dato', 'Dato')
        c_dato_slutt = cols.get('slutt_dato', 'slutt_dato')
        c_finnkode = cols.get('finnkode', 'FinnKode')
        c_overskrift = cols.get('overskrift', 'Overskrift')

        # Sikre numeriske verdier for beregning
        df_filtered[c_pris_start] = pd.to_numeric(df_filtered.get(c_pris_start, 0), errors='coerce').fillna(0)
        df_filtered[c_pris_slutt] = pd.to_numeric(df_filtered.get(c_pris_slutt, 0), errors='coerce').fillna(0)

        # Beregn dager
        if c_dato_start in df_filtered.columns and c_dato_slutt in df_filtered.columns:
            df_filtered['dager'] = (df_filtered[c_dato_slutt] - df_filtered[c_dato_start]).dt.days
            df_filtered['dager'] = df_filtered['dager'].apply(lambda x: x if x > 0 else 0)
        else:
            df_filtered['dager'] = 0

        # --- BEREGN ENDRET PRIS ---
        # Positivt tall betyr at prisen har økt. Negativt betyr at den har falt.
        df_filtered['prisfall'] = df_filtered[c_pris_slutt] - df_filtered[c_pris_start]

        # --- LAG FINN-LINK ---
        # Tving FinnKode til string uten desimaler (.0)
        df_filtered[c_finnkode] = df_filtered[c_finnkode].astype(str).str.replace(r'\.0$', '', regex=True)
        df_filtered['finn_url'] = FINN_BASE_URL + df_filtered[c_finnkode]

        # --- FORBERED JSON ---
        # Her mapper vi databasens kolonnenavn til det HTML/JS forventer
        # 'pris_last' er nøkkelen frontend bruker for "Siste pris"
        # 'overskrift' er nøkkelen frontend bruker for Tittel
        rename_map = {
            'Produsent': 'produsent',
            'Modell': 'modell',
            c_overskrift: 'overskrift',  # <-- Tittelen din
            'årstall': 'årstall',
            'kjørelengde': 'kjørelengde',
            'drivstoff': 'drivstoff',
            'hjuldrift': 'hjuldrift',
            'rekkevidde_str': 'rekkevidde',
            'selger': 'selger',
            c_dato_start: 'dato_start',
            c_dato_slutt: 'dato_end',
            c_pris_start: 'pris_start',
            c_pris_slutt: 'pris_last',  # <-- Siste pris
            c_finnkode: 'finnkode'
        }

        # Bruk bare de kolonnene som finnes
        actual_rename = {k: v for k, v in rename_map.items() if k in df_filtered.columns}
        output_df = df_filtered.rename(columns=actual_rename)

        # Sortering (Nyeste salg øverst, eller høyest pris)
        if 'pris_last' in output_df.columns:
            output_df = output_df.sort_values('pris_last', ascending=True)

        # --- KPIer ---
        kpis = {}
        if 'pris_last' in output_df.columns:
            solgte = output_df[output_df['pris_last'] > 1000]
            if not solgte.empty:
                kpis = {
                    'avg_dager': int(solgte['dager'].mean()),
                    'median_dager': int(solgte['dager'].median()),
                    'avg_pris': int(solgte['pris_last'].mean()),
                    'median_pris': int(solgte['pris_last'].median()),
                    'laveste_pris': int(solgte['pris_last'].min()),
                    'antall': len(solgte)
                }

            # --- Grafer ---
            if 'dato_end' in solgte.columns:
                daily_stats_df = solgte.groupby(solgte['dato_end'].dt.date).agg(
                    Antall_Solgt=('pris_last', 'count'),
                    Median_Pris_Usolgt=('pris_last', 'median')
                ).reset_index()
                daily_stats_df.rename(columns={'dato_end': 'Dato'}, inplace=True)
                daily_stats_df['Dato'] = pd.to_datetime(daily_stats_df['Dato']).dt.strftime('%Y-%m-%d')
                daily_stats = json.loads(daily_stats_df.to_json(orient='records'))
            else:
                daily_stats = []
        else:
            daily_stats = []

        # JSON Export
        output_df = output_df.where(pd.notna(output_df), None)
        if len(output_df) > 2000:
            output_df = output_df.head(2000)

        historikk = json.loads(output_df.to_json(orient='records', date_format='iso'))

        return jsonify({'historikk': historikk, 'daily_stats': daily_stats, 'kpis': kpis})

    except Exception as e:
        print(f"Feil i /bil/solgt/data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


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
            return jsonify({'rows': [], 'kpis': {}})

        vis_solgte = vis_solgte.where(pd.notna(vis_solgte), None)
        rows = json.loads(vis_solgte.to_json(orient='records'))
        return jsonify({'rows': rows, 'kpis': {}})

    except Exception as e:
        print(f"Feil i /bil/rekordrask/data: {e}")
        return jsonify({"error": str(e)}), 500


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
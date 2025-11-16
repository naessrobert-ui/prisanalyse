# bolig_routes.py
import io
import json

import boto3
import pandas as pd
from flask import Blueprint, render_template, jsonify, request

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME
from helpers import find_latest_file_in_s3

bolig_bp = Blueprint('bolig', __name__, url_prefix='/bolig')


@bolig_bp.route('/')
def bolig_analyse_side():
    """Viser analysesiden for boliger for salg."""
    try:
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET
        )

        latest_file_key = find_latest_file_in_s3(
            s3_client,
            S3_BUCKET_NAME,
            'raw/bolig-daglig/',
            r'bolig_X_(\d{2}-\d{2}-\d{4})\.csv'
        )

        filter_data = {'fylker': [], 'boligtyper': [], 'meglere': [], 'annonsepakker': []}

        if latest_file_key:
            obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=latest_file_key)
            df = pd.read_csv(
                io.BytesIO(obj['Body'].read()),
                sep=';',
                encoding='utf-16',
                on_bad_lines='skip'
            )
            df.columns = df.columns.str.strip()

            if 'fylke' in df.columns:
                filter_data['fylker'] = sorted(df['fylke'].dropna().unique().tolist())
            if 'boligtype' in df.columns:
                filter_data['boligtyper'] = sorted(df['boligtype'].dropna().unique().tolist())
            if 'broker_name' in df.columns:
                filter_data['meglere'] = sorted(df['broker_name'].dropna().unique().tolist())
            if 'annonsepakke' in df.columns:
                filter_data['annonsepakker'] = sorted(df['annonsepakke'].dropna().unique().tolist())

    except Exception as e:
        print(f"Feil under forberedelse av bolig-filtre: {e}")
        filter_data = {'fylker': [], 'boligtyper': [], 'meglere': [], 'annonsepakker': []}

    return render_template(
        'analyse_template.html',
        tittel="Prisanalyse: Boliger for salg i Norge",
        data_url="/bolig/data",
        show_fritidsbolig_link=True,# <- NY URL-STI
        **filter_data
    )


@bolig_bp.route('/data', methods=['POST'])
def get_bolig_data():
    """API-endepunkt som henter og filtrerer boligdata fra S3."""
    try:
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET
        )

        latest_file_key = find_latest_file_in_s3(
            s3_client,
            S3_BUCKET_NAME,
            'raw/bolig-daglig/',
            r'bolig_X_(\d{2}-\d{2}-\d{4})\.csv'
        )
        if not latest_file_key:
            return jsonify({"error": "Ingen bolig-datafil funnet"}), 404

        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=latest_file_key)
        df = pd.read_csv(
            io.BytesIO(obj['Body'].read()),
            sep=';',
            encoding='utf-16',
            on_bad_lines='skip'
        )
        df.columns = df.columns.str.strip()

        if 'publisert_dato' in df.columns:
            df['publisert_dato_dt'] = pd.to_datetime(df['publisert_dato'], errors='coerce', utc=True)
            now_utc = pd.Timestamp.now('UTC')
            df['dager_paa_markedet'] = (now_utc - df['publisert_dato_dt']).dt.days
        else:
            df['dager_paa_markedet'] = None

        filters = request.get_json().get('filters', {})

        for col in ['totalpris', 'M2-pris', 'dager_paa_markedet']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if filters.get('fylke') and filters['fylke'] != 'Alle':
            df = df[df['fylke'] == filters['fylke']]
        if filters.get('totalpris_fra'):
            df = df[df['totalpris'] >= int(filters['totalpris_fra'])]
        if filters.get('totalpris_til'):
            df = df[df['totalpris'] <= int(filters['totalpris_til'])]
        if filters.get('dager_fra'):
            df = df[df['dager_paa_markedet'] >= int(filters['dager_fra'])]
        if filters.get('dager_til'):
            df = df[df['dager_paa_markedet'] <= int(filters['dager_til'])]
        if filters.get('m2pris_fra'):
            df = df[df['M2-pris'] >= int(filters['m2pris_fra'])]
        if filters.get('m2pris_til'):
            df = df[df['M2-pris'] <= int(filters['m2pris_til'])]
        if filters.get('boligtype') and filters['boligtype'] != 'Alle':
            df = df[df['boligtype'] == filters['boligtype']]
        if filters.get('megler') and filters['megler'] != 'Alle':
            df = df[df['broker_name'] == filters['megler']]
        if filters.get('annonsepakke') and filters['annonsepakke'] != 'Alle':
            df = df[df['annonsepakke'] == filters['annonsepakke']]

        if filters.get('keyword'):
            search_term = filters['keyword']
            if 'full_title' in df.columns:
                df = df[df['full_title'].astype(str).str.contains(search_term, case=False, na=False)]

        df = df.where(pd.notna(df), None)
        return jsonify(json.loads(df.to_json(orient='records')))

    except Exception as e:
        print(f"Feil i /bolig/data: {e}")
        return jsonify({"error": "Intern feil"}), 500

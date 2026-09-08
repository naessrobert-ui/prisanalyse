# -*- coding: utf-8 -*-
import html
import json
import re
import uuid
from datetime import datetime, timezone

import boto3
from flask import Blueprint, Response, jsonify, request

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

innbytte_leads_bp = Blueprint("innbytte_leads", __name__)
LEAD_PREFIX = "calc/bil/innbytte_leads/"


def _s3():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )


def _clean(value, max_len=500):
    return (value or "").strip()[:max_len]


def _valid_email(value):
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", value or ""))


@innbytte_leads_bp.post("/innbytte/lead")
def submit_innbytte_lead():
    if _clean(request.form.get("website"), 100):
        return jsonify({"ok": True})

    kind = _clean(request.form.get("kind"), 20)
    if kind not in {"tilbud", "prisfeedback"}:
        return jsonify({"ok": False, "error": "Ugyldig henvendelse."}), 400

    navn = _clean(request.form.get("navn"), 120)
    telefon = _clean(request.form.get("telefon"), 50)
    email = _clean(request.form.get("email"), 160)
    samtykke = _clean(request.form.get("samtykke"), 10)

    if not navn or not telefon or not email:
        return jsonify({"ok": False, "error": "Navn, telefon og e-post må fylles ut."}), 400
    if not _valid_email(email):
        return jsonify({"ok": False, "error": "Skriv inn en gyldig e-postadresse."}), 400
    if len(re.sub(r"\D", "", telefon)) < 5:
        return jsonify({"ok": False, "error": "Skriv inn et gyldig telefonnummer."}), 400
    if samtykke != "ja":
        return jsonify({"ok": False, "error": "Du må samtykke til at vi kan kontakte deg om henvendelsen."}), 400

    forventet_pris = _clean(request.form.get("forventet_pris"), 60)
    kommentar = _clean(request.form.get("kommentar"), 2000)
    if kind == "prisfeedback" and not forventet_pris:
        return jsonify({"ok": False, "error": "Oppgi hvilket prisnivå du mener er riktig."}), 400

    now = datetime.now(timezone.utc)
    payload = {
        "id": uuid.uuid4().hex,
        "type": kind,
        "opprettet_utc": now.isoformat(),
        "navn": navn,
        "telefon": telefon,
        "email": email,
        "samtykke": True,
        "regnr": _clean(request.form.get("regnr"), 20).upper(),
        "km": _clean(request.form.get("km"), 30),
        "bil": _clean(request.form.get("bil"), 200),
        "estimert_innbyttepris": _clean(request.form.get("estimat"), 60),
        "markedsniva": _clean(request.form.get("markedsniva"), 60),
        "forventet_pris": forventet_pris,
        "kommentar": kommentar,
        "side": _clean(request.form.get("side"), 200),
    }

    key = f"{LEAD_PREFIX}{now:%Y/%m/%d}/{now:%H%M%S}_{payload['id']}.json"
    try:
        _s3().put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except Exception as exc:
        print(f"[innbytte-lead] Kunne ikke lagre lead: {exc!r}")
        return jsonify({"ok": False, "error": "Kunne ikke sende henvendelsen akkurat nå. Prøv igjen litt senere."}), 500

    if kind == "tilbud":
        msg = "Takk! Vi har mottatt kontaktinformasjonen din og kan følge opp med et uforpliktende pristilbud."
    else:
        msg = "Takk! Tilbakemeldingen er mottatt. Den hjelper oss å forbedre prisestimatene."
    return jsonify({"ok": True, "message": msg})


@innbytte_leads_bp.get("/bil/innbytte/leads")
def innbytte_leads_admin():
    leads = []
    try:
        s3 = _s3()
        keys = []
        token = None
        while len(keys) < 250:
            kwargs = {"Bucket": S3_BUCKET_NAME, "Prefix": LEAD_PREFIX, "MaxKeys": 250}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            keys.extend([x["Key"] for x in resp.get("Contents", [])])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        for key in sorted(keys, reverse=True)[:200]:
            try:
                obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                leads.append(json.loads(obj["Body"].read().decode("utf-8")))
            except Exception:
                continue
    except Exception as exc:
        return Response(
            f"Kunne ikke hente henvendelser: {html.escape(str(exc))}",
            status=500,
            mimetype="text/plain",
        )

    def e(value):
        return html.escape(str(value or ""))

    rows = []
    for item in leads:
        typ = "Pristilbud" if item.get("type") == "tilbud" else "Prisfeedback"
        kommentar = item.get("kommentar") or ""
        if item.get("forventet_pris"):
            kommentar = f"Riktig pris: {item.get('forventet_pris')} — {kommentar}"
        rows.append(
            "<tr>"
            f"<td>{e(item.get('opprettet_utc'))}</td>"
            f"<td>{e(typ)}</td>"
            f"<td><strong>{e(item.get('bil'))}</strong><br>{e(item.get('regnr'))} · {e(item.get('km'))} km</td>"
            f"<td>{e(item.get('estimert_innbyttepris'))}<br><small>Marked: {e(item.get('markedsniva'))}</small></td>"
            f"<td>{e(item.get('navn'))}<br>{e(item.get('telefon'))}<br><a href='mailto:{e(item.get('email'))}'>{e(item.get('email'))}</a></td>"
            f"<td>{e(kommentar)}</td>"
            "</tr>"
        )

    page = f'''<!doctype html><html lang="nb"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Innbyttehenvendelser</title>
<style>body{{font-family:system-ui;background:#071016;color:#eef4f7;margin:0;padding:28px}}a{{color:#a7ef5a}}h1{{margin-top:0}}table{{width:100%;border-collapse:collapse;background:#0d1a22}}th,td{{padding:11px;border:1px solid #263943;text-align:left;vertical-align:top;font-size:13px}}th{{color:#a7ef5a}}small{{color:#9db3bf}}</style></head><body>
<h1>Innbyttehenvendelser</h1><p>{len(leads)} siste henvendelser.</p><table><thead><tr><th>Tid</th><th>Type</th><th>Bil</th><th>Pris</th><th>Kontakt</th><th>Kommentar</th></tr></thead><tbody>{''.join(rows)}</tbody></table></body></html>'''
    return Response(page, mimetype="text/html")

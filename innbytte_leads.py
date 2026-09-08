# -*- coding: utf-8 -*-
import html
import json
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from flask import Blueprint, Response, jsonify, request
from werkzeug.utils import secure_filename

from config import AWS_KEY, AWS_SECRET, AWS_REGION, S3_BUCKET_NAME

innbytte_leads_bp = Blueprint("innbytte_leads", __name__)
LEAD_PREFIX = "calc/bil/innbytte_leads/"
IMAGE_PREFIX = "calc/bil/innbytte_lead_images/"
PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"
LEADS_URL = "https://prisanalyse.no/bil/innbytte/leads"
MAX_IMAGES = 8
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_REQUEST_BYTES = 85 * 1024 * 1024
ALLOWED_IMAGE_TYPES = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
    "image/heic",
    "image/heif",
}


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


def _delete_uploaded_images(s3, keys):
    for key in keys:
        try:
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        except Exception as exc:
            print(f"[innbytte-lead] Kunne ikke rydde opp bilde {key}: {exc!r}")


def _store_images(s3, lead_id):
    files = [f for f in request.files.getlist("bilder") if f and f.filename]
    if len(files) > MAX_IMAGES:
        return None, "Du kan laste opp maksimalt 8 bilder."

    records = []
    uploaded_keys = []
    for index, file in enumerate(files, start=1):
        content_type = (file.mimetype or "").lower().strip()
        if content_type not in ALLOWED_IMAGE_TYPES:
            _delete_uploaded_images(s3, uploaded_keys)
            return None, "Bildene må være JPEG, PNG, WebP eller HEIC/HEIF."

        data = file.read(MAX_IMAGE_BYTES + 1)
        if len(data) > MAX_IMAGE_BYTES:
            _delete_uploaded_images(s3, uploaded_keys)
            return None, "Hvert bilde kan være maksimalt 10 MB."
        if not data:
            continue

        safe_name = secure_filename(file.filename) or f"bilde-{index}.jpg"
        key = f"{IMAGE_PREFIX}{lead_id}/{index:02d}-{safe_name}"
        try:
            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Body=data,
                ContentType=content_type,
                ContentDisposition=f'inline; filename="{safe_name}"',
            )
        except Exception as exc:
            _delete_uploaded_images(s3, uploaded_keys)
            print(f"[innbytte-lead] Bildeopplasting feilet: {exc!r}")
            return None, "Kunne ikke laste opp bildene akkurat nå. Prøv igjen litt senere."

        uploaded_keys.append(key)
        records.append(
            {
                "key": key,
                "filename": safe_name,
                "content_type": content_type,
                "size": len(data),
            }
        )

    return records, None


def _send_pushover_lead(payload):
    """Send push-varsel om ny henvendelse. Feil her skal aldri miste leadet."""
    token = (os.environ.get("PUSHOVER_TOKEN") or "").strip()
    users_raw = (os.environ.get("PUSHOVER_USER") or "").strip()
    users = [user.strip() for user in users_raw.split(",") if user.strip()]

    if not token or not users:
        print("[innbytte-lead] Pushover er ikke konfigurert i Render (PUSHOVER_TOKEN/PUSHOVER_USER).")
        return False

    regnr = payload.get("regnr") or "ukjent regnr"
    kind = payload.get("type")
    if kind == "tilbud":
        title = f"🚗 Ny forespørsel om pristilbud – {regnr}"
    else:
        title = f"💬 Prisfeedback – {regnr}"

    lines = []
    bil = payload.get("bil") or "Bil"
    km = payload.get("km")
    lines.append(f"{bil} · {regnr}" + (f" · {km} km" if km else ""))

    if payload.get("estimert_innbyttepris"):
        lines.append(f"Vårt estimat: {payload['estimert_innbyttepris']} kr")
    if payload.get("markedsniva"):
        lines.append(f"Markedsnivå: {payload['markedsniva']} kr")
    if payload.get("forventet_pris"):
        lines.append(f"Bruker mener riktig pris er: {payload['forventet_pris']} kr")

    lines.extend([
        f"Navn: {payload.get('navn') or '–'}",
        f"Telefon: {payload.get('telefon') or '–'}",
        f"E-post: {payload.get('email') or '–'}",
    ])

    if payload.get("ekstrautstyr"):
        lines.append(f"Utstyr: {payload['ekstrautstyr'][:180]}")
    if payload.get("skader"):
        lines.append(f"Skader/mangler: {payload['skader'][:180]}")
    if payload.get("serviceinfo"):
        lines.append(f"Service/batteri: {payload['serviceinfo'][:180]}")

    kommentar = (payload.get("kommentar") or "").strip()
    if kommentar:
        lines.append(f"Kommentar: {kommentar[:220]}")

    image_count = len(payload.get("bilder") or [])
    if image_count:
        lines.append(f"📷 {image_count} bilde{'r' if image_count != 1 else ''} vedlagt")

    message = "\n".join(lines)[:1024]
    title = title[:250]
    ok = True

    for user in users:
        data = urlencode({
            "token": token,
            "user": user,
            "title": title,
            "message": message,
            "url": LEADS_URL,
            "url_title": "Åpne innbyttehenvendelser",
        }).encode("utf-8")
        try:
            req = Request(
                PUSHOVER_API_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urlopen(req, timeout=8) as resp:
                if not 200 <= int(resp.status) < 300:
                    raise RuntimeError(f"Pushover HTTP {resp.status}")
        except Exception as exc:
            ok = False
            print(f"[innbytte-lead] Pushover-varsel feilet: {exc!r}")

    return ok


@innbytte_leads_bp.post("/innbytte/lead")
def submit_innbytte_lead():
    if request.content_length and request.content_length > MAX_REQUEST_BYTES:
        return jsonify({"ok": False, "error": "Opplastingen er for stor. Reduser antall bilder eller bildestørrelsen."}), 413

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
    ekstrautstyr = _clean(request.form.get("ekstrautstyr"), 2500)
    skader = _clean(request.form.get("skader"), 2500)
    serviceinfo = _clean(request.form.get("serviceinfo"), 2500)
    if kind == "prisfeedback" and not forventet_pris:
        return jsonify({"ok": False, "error": "Oppgi hvilket prisnivå du mener er riktig."}), 400

    now = datetime.now(timezone.utc)
    lead_id = uuid.uuid4().hex
    s3 = _s3()
    images, image_error = _store_images(s3, lead_id)
    if image_error:
        return jsonify({"ok": False, "error": image_error}), 400

    payload = {
        "id": lead_id,
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
        "ekstrautstyr": ekstrautstyr,
        "skader": skader,
        "serviceinfo": serviceinfo,
        "kommentar": kommentar,
        "bilder": images or [],
        "side": _clean(request.form.get("side"), 200),
    }

    key = f"{LEAD_PREFIX}{now:%Y/%m/%d}/{now:%H%M%S}_{payload['id']}.json"
    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except Exception as exc:
        _delete_uploaded_images(s3, [item["key"] for item in (images or [])])
        print(f"[innbytte-lead] Kunne ikke lagre lead: {exc!r}")
        return jsonify({"ok": False, "error": "Kunne ikke sende henvendelsen akkurat nå. Prøv igjen litt senere."}), 500

    # Leadet er trygt lagret før vi varsler. Pushover-feil påvirker derfor ikke kunden.
    _send_pushover_lead(payload)

    if kind == "tilbud":
        msg = "Takk! Vi har mottatt opplysningene og kan følge opp med et uforpliktende pristilbud."
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

        bilinfo = []
        if item.get("ekstrautstyr"):
            bilinfo.append(f"<strong>Utstyr:</strong> {e(item.get('ekstrautstyr'))}")
        if item.get("skader"):
            bilinfo.append(f"<strong>Skader/mangler:</strong> {e(item.get('skader'))}")
        if item.get("serviceinfo"):
            bilinfo.append(f"<strong>Service/batteri:</strong> {e(item.get('serviceinfo'))}")
        bilinfo_html = "<br><br>".join(bilinfo) if bilinfo else "–"

        image_links = []
        for idx, image in enumerate(item.get("bilder") or [], start=1):
            image_key = image.get("key") if isinstance(image, dict) else None
            if not image_key:
                continue
            try:
                url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET_NAME, "Key": image_key},
                    ExpiresIn=3600,
                )
                name = image.get("filename") or f"Bilde {idx}"
                image_links.append(
                    f"<a href='{e(url)}' target='_blank' rel='noopener'>📷 {e(name)}</a>"
                )
            except Exception:
                continue
        images_html = "<br>".join(image_links) if image_links else "–"

        rows.append(
            "<tr>"
            f"<td>{e(item.get('opprettet_utc'))}</td>"
            f"<td>{e(typ)}</td>"
            f"<td><strong>{e(item.get('bil'))}</strong><br>{e(item.get('regnr'))} · {e(item.get('km'))} km</td>"
            f"<td>{e(item.get('estimert_innbyttepris'))}<br><small>Marked: {e(item.get('markedsniva'))}</small></td>"
            f"<td>{e(item.get('navn'))}<br>{e(item.get('telefon'))}<br><a href='mailto:{e(item.get('email'))}'>{e(item.get('email'))}</a></td>"
            f"<td class='wrap'>{bilinfo_html}</td>"
            f"<td class='wrap'>{e(kommentar)}</td>"
            f"<td>{images_html}</td>"
            "</tr>"
        )

    page = f'''<!doctype html><html lang="nb"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Innbyttehenvendelser</title>
<style>body{{font-family:system-ui;background:#071016;color:#eef4f7;margin:0;padding:28px}}a{{color:#a7ef5a}}h1{{margin-top:0}}.table-wrap{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;background:#0d1a22;min-width:1350px}}th,td{{padding:11px;border:1px solid #263943;text-align:left;vertical-align:top;font-size:13px}}th{{color:#a7ef5a}}small{{color:#9db3bf}}td.wrap{{white-space:normal;min-width:240px;line-height:1.45}}</style></head><body>
<h1>Innbyttehenvendelser</h1><p>{len(leads)} siste henvendelser.</p><div class="table-wrap"><table><thead><tr><th>Tid</th><th>Type</th><th>Bil</th><th>Pris</th><th>Kontakt</th><th>Bilopplysninger</th><th>Kommentar</th><th>Bilder</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div></body></html>'''
    return Response(page, mimetype="text/html")

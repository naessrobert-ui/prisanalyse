# -*- coding: utf-8 -*-
"""Public trip page and private photo upload for South England 2026."""

from __future__ import annotations

import datetime as dt
import hmac
import io
import json
import os
import uuid
from urllib.parse import quote
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from PIL import Image, ImageOps, UnidentifiedImageError

ferie_bp = Blueprint("ferie", __name__, url_prefix="/ferie")

TRIP_SLUG = "sor-england-2026"
TRIP_TITLE = "Sør-England 2026"
LONDON_TZ = ZoneInfo("Europe/London")
DEFAULT_S3_PREFIX = "travel/south-england-2026"
MANIFEST_FILE = "manifest.json"
MAX_FILES_PER_UPLOAD = 8
MAX_PHOTO_BYTES = 15 * 1024 * 1024
MAX_IMAGE_EDGE = 2000


def _maps_url(query: str) -> str:
    return f"https://www.google.com/maps/search/?api=1&query={quote(query)}"


def _directions_url(origin: str, destination: str, travelmode: str = "driving") -> str:
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={quote(origin)}&destination={quote(destination)}&travelmode={travelmode}"
    )


ITINERARY = [
    {
        "date": "2026-07-19",
        "weekday": "Søndag",
        "place": "Gatwick → Eastbourne",
        "stay": "Hydro Hotel",
        "summary": "Flyankomst, direkte tog til Eastbourne og taxi til hotellet.",
        "items": [
            {"time": "20:20", "title": "Lander på Gatwick", "detail": "Lokal britisk tid. Følg skiltene til togstasjonen i terminalen."},
            {"time": "ca. 21:15", "title": "Tog mot Eastbourne", "detail": "Direkte tog bruker normalt rundt 53 minutter. Sjekk liveavganger etter landing."},
            {"time": "ca. 22:20", "title": "Taxi til Hydro Hotel", "detail": "Kort taxitur fra Eastbourne stasjon. Sen innsjekking og rolig kveld."},
        ],
        "links": [
            {"label": "Gatwick Airport", "url": _maps_url("Gatwick Airport railway station")},
            {"label": "Hydro Hotel", "url": _maps_url("Hydro Hotel Eastbourne")},
            {"label": "Toginformasjon", "url": "https://www.southernrailway.com/journey/gatwick-airport-to-eastbourne"},
        ],
    },
    {
        "date": "2026-07-20",
        "weekday": "Mandag",
        "place": "Eastbourne og Seven Sisters",
        "stay": "Hydro Hotel",
        "summary": "Turens store kystvandring, deretter hage og utendørsbasseng.",
        "items": [
            {"time": "08:30", "title": "Frokost på Hydro", "detail": "Rolig start. Ta med vann, solkrem, vindjakke og glutenfri mat."},
            {"time": "10:15", "title": "Til Seven Sisters Country Park", "detail": "Taxi eller buss til Gildredge Road og Coaster 12/12X til Exceat."},
            {"time": "11:00–15:30", "title": "Seven Sisters til Eastbourne", "detail": "Ca. 7–8 miles / 11–13 km, 3–4 timer uten lange stopp. Birling Gap og Beachy Head underveis."},
            {"time": "16:00", "title": "Hage og basseng", "detail": "Tilbake på Hydro. Den fineste avslutningen på en varm sommerdag."},
            {"time": "19:30", "title": "Middag", "detail": "Hydro eller Eastbourne sentrum. Opplys tydelig om cøliaki."},
        ],
        "links": [
            {"label": "Start ved Exceat", "url": _maps_url("Seven Sisters Country Park Visitor Centre")},
            {"label": "Birling Gap", "url": _maps_url("Birling Gap National Trust")},
            {"label": "Offisiell turbeskrivelse", "url": "https://www.southdowns.gov.uk/walks/south-downs-way-walk-seven-sisters-to-eastbourne/"},
        ],
    },
    {
        "date": "2026-07-21",
        "weekday": "Tirsdag",
        "place": "Eastbourne → Brighton → Battle → Rye",
        "stay": "The Hope Anchor, Rye",
        "summary": "Løpetur og tog til Brighton, hente leiebil og sightseeing på vei til Rye.",
        "items": [
            {"time": "07:00", "title": "Robert løper mot Brighton", "detail": "Forslag: kysten til Seaford, omtrent 20–22 km og kupert. Tog videre fra Seaford til Brighton."},
            {"time": "10:00", "title": "Utsjekking fra Hydro", "detail": "Helene tar bagasjen med tog fra Eastbourne til Brighton."},
            {"time": "ca. 11:30", "title": "Hent leiebilen i Brighton", "detail": "Møtes ved utleiekontoret. Husk førerkort, pass og betalingskort i hovedsjåførens navn."},
            {"time": "13:00–15:30", "title": "Battle Abbey", "detail": "Slagmarken fra 1066, klosterruinene og portbygningen. Beregn rundt to timer."},
            {"time": "16:00", "title": "Hastings – valgfritt stopp", "detail": "Kort rusletur og kaffe i Old Town. Dropp stoppet dersom dagen har blitt lang."},
            {"time": "17:30–18:30", "title": "Ankomst Rye", "detail": "Innsjekking på The Hope Anchor. Deluxe Double Room med frokost."},
        ],
        "links": [
            {"label": "Løpemål Seaford", "url": _maps_url("Seaford railway station East Sussex")},
            {"label": "Battle Abbey", "url": _maps_url("1066 Battle of Hastings Abbey and Battlefield")},
            {"label": "Kjørerute", "url": _directions_url("Brighton", "The Hope Anchor Rye")},
        ],
    },
    {
        "date": "2026-07-22",
        "weekday": "Onsdag",
        "place": "Bodiam, vingård og Rye",
        "stay": "The Hope Anchor, Rye",
        "summary": "Middelalderslott om formiddagen, engelsk vin og rolig ettermiddag i gamlebyen.",
        "items": [
            {"time": "08:30", "title": "Frokost", "detail": "Kjør fra Rye rundt 09:20."},
            {"time": "10:00–11:45", "title": "Bodiam Castle", "detail": "Slott med vollgrav. Åpent 10–17; forhåndsbestilling er normalt ikke nødvendig."},
            {"time": "12:15", "title": "Tilbake til Rye", "detail": "Parker bilen for resten av dagen."},
            {"time": "13:00", "title": "Charles Palmer Wine & Cheese", "detail": "Fire viner og fire oster. Må forhåndsbestilles. Ta taxi begge veier slik at begge kan smake."},
            {"time": "15:00–18:00", "title": "Rye gamleby", "detail": "Mermaid Street, St Mary’s Church, småbutikker og utsikt over Rother-landskapet."},
            {"time": "19:30", "title": "Middag i Rye", "detail": "Bestill bord og avklar cøliaki og krysskontaminering på forhånd."},
        ],
        "links": [
            {"label": "Bodiam Castle", "url": _maps_url("Bodiam Castle")},
            {"label": "Charles Palmer", "url": "https://charlespalmer-vineyards.co.uk/pages/wine-tastings"},
            {"label": "Mermaid Street", "url": _maps_url("Mermaid Street Rye")},
        ],
    },
    {
        "date": "2026-07-23",
        "weekday": "Torsdag",
        "place": "Rye → Brighton",
        "stay": "Brighton – balkong og sjøutsikt",
        "summary": "Rolig morgen, retur langs kysten, bilinnlevering kl. 15 og familien i Brighton.",
        "items": [
            {"time": "08:30", "title": "Frokost og utsjekking", "detail": "Avreise rundt 10. Det er god margin til innleveringen."},
            {"time": "10:00–13:15", "title": "Kjør mot Brighton", "detail": "Valgfri kort pause i Lewes dersom trafikken og dagsformen tillater det."},
            {"time": "13:30", "title": "Bagasje til hotellet", "detail": "Sjekk inn hvis rommet er klart, ellers sett fra dere bagasjen."},
            {"time": "15:00", "title": "Lever leiebilen", "detail": "Fast frist. Legg inn 30–45 minutters sikkerhetsmargin."},
            {"time": "fra 15:30", "title": "Brighton med familien", "detail": "Balkong, sjøutsikt, The Lanes og middag sammen."},
        ],
        "links": [
            {"label": "Kjørerute til Brighton", "url": _directions_url("The Hope Anchor Rye", "Brighton")},
            {"label": "Lewes – valgfritt", "url": _maps_url("Lewes East Sussex")},
            {"label": "The Lanes", "url": _maps_url("The Lanes Brighton")},
        ],
    },
    {
        "date": "2026-07-24",
        "weekday": "Fredag",
        "place": "Brighton → London",
        "stay": "London",
        "summary": "Formiddag og ettermiddag med barna og barnebarna, deretter tog til London.",
        "items": [
            {"time": "08:30", "title": "Frokost", "detail": "Sjekk ut og la bagasjen stå på hotellet."},
            {"time": "10:30–16:30", "title": "Familiedag i Brighton", "detail": "Strandpromenaden, piren eller en lang familielunsj – hold dagen enkel."},
            {"time": "ca. 17:00", "title": "Tog til London", "detail": "Direkte til Victoria eller London Bridge, avhengig av hvor dere skal bo."},
        ],
        "links": [
            {"label": "Brighton Palace Pier", "url": _maps_url("Brighton Palace Pier")},
            {"label": "Tog til London", "url": "https://www.southernrailway.com/journey/brighton-to-london-victoria"},
        ],
    },
]

BOOKINGS = [
    {"category": "Fly", "name": "Gatwick", "dates": "Søn. 19. juli kl. 20:20", "status": "Bekreftet", "note": "Flynummer kan legges inn senere."},
    {"category": "Hotell", "name": "Hydro Hotel, Eastbourne", "dates": "19.–21. juli · 2 netter", "status": "Bestilt", "note": "Hage og oppvarmet utendørsbasseng."},
    {"category": "Hotell", "name": "The Hope Anchor, Rye", "dates": "21.–23. juli · 2 netter", "status": "Bestilt", "note": "Deluxe Double Room, 30 m², frokost inkludert."},
    {"category": "Bil", "name": "Leiebil fra Brighton", "dates": "21.–23. juli", "status": "Planlagt", "note": "Leveres i Brighton torsdag senest kl. 15:00."},
    {"category": "Hotell", "name": "Brighton", "dates": "23.–24. juli · 1 natt", "status": "Bestilt", "note": "Balkong og sjøutsikt. Hotellnavn kan legges inn."},
    {"category": "Opplevelse", "name": "Charles Palmer Vineyards", "dates": "Ons. 22. juli kl. 13:00", "status": "Kontroller", "note": "Wine & Cheese Experience må forhåndsbestilles."},
]


def _s3_bucket() -> str:
    return (os.getenv("FERIE_S3_BUCKET") or os.getenv("S3_BUCKET_NAME") or "").strip()


def _s3_prefix() -> str:
    return (os.getenv("FERIE_S3_PREFIX") or DEFAULT_S3_PREFIX).strip("/")


def _s3_client():
    region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "").strip()
    kwargs = {"region_name": region} if region else {}
    return boto3.client("s3", **kwargs)


def _manifest_key() -> str:
    return f"{_s3_prefix()}/{MANIFEST_FILE}"


def _missing_object(exc: Exception) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"404", "NoSuchKey", "NotFound"}


def _read_manifest(client, bucket: str) -> list[dict]:
    try:
        obj = client.get_object(Bucket=bucket, Key=_manifest_key())
    except ClientError as exc:
        if _missing_object(exc):
            return []
        raise
    payload = json.loads(obj["Body"].read().decode("utf-8"))
    entries = payload.get("photos", []) if isinstance(payload, dict) else []
    return [item for item in entries if isinstance(item, dict) and item.get("key")]


def _write_manifest(client, bucket: str, entries: list[dict]) -> None:
    payload = {
        "trip": TRIP_SLUG,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "photos": entries[:250],
    }
    client.put_object(
        Bucket=bucket,
        Key=_manifest_key(),
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
        CacheControl="no-cache",
    )


def _list_photos() -> tuple[list[dict], str | None]:
    bucket = _s3_bucket()
    if not bucket:
        return [], "Bildearkivet aktiveres når S3-bucket er konfigurert."
    try:
        client = _s3_client()
        entries = _read_manifest(client, bucket)
        result = []
        for item in entries[:80]:
            key = str(item.get("key", ""))
            if not key.startswith(f"{_s3_prefix()}/photos/"):
                continue
            enriched = dict(item)
            enriched["url"] = client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=3600,
            )
            result.append(enriched)
        return result, None
    except (BotoCoreError, ClientError, ValueError, json.JSONDecodeError) as exc:
        current_app.logger.warning("Kunne ikke laste feriebilder: %s", exc)
        return [], "Bildearkivet er midlertidig utilgjengelig."


def _prepare_jpeg(raw: bytes) -> bytes:
    if len(raw) > MAX_PHOTO_BYTES:
        raise ValueError("Bildet er større enn 15 MB.")
    try:
        with Image.open(io.BytesIO(raw)) as image:
            image = ImageOps.exif_transpose(image)
            if getattr(image, "is_animated", False):
                image.seek(0)
            image = image.convert("RGB")
            image.thumbnail((MAX_IMAGE_EDGE, MAX_IMAGE_EDGE), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            image.save(output, format="JPEG", quality=87, optimize=True, progressive=True)
            return output.getvalue()
    except (UnidentifiedImageError, OSError) as exc:
        raise ValueError("Filen kunne ikke leses som et bilde.") from exc


def _upload_enabled() -> bool:
    return bool((os.getenv("FERIE_UPLOAD_CODE") or "").strip() and _s3_bucket())


def _current_day() -> tuple[dict, bool]:
    today = dt.datetime.now(LONDON_TZ).date()
    parsed = [(dt.date.fromisoformat(item["date"]), item) for item in ITINERARY]
    for day, item in parsed:
        if day == today:
            return item, True
    if today < parsed[0][0]:
        return parsed[0][1], False
    if today > parsed[-1][0]:
        return parsed[-1][1], False
    future = next(item for day, item in parsed if day > today)
    return future, False


@ferie_bp.route(f"/{TRIP_SLUG}/")
def south_england_trip():
    photos, gallery_message = _list_photos()
    active_day, is_today = _current_day()
    return render_template(
        "ferie_sor_england_2026.html",
        trip_title=TRIP_TITLE,
        itinerary=ITINERARY,
        bookings=BOOKINGS,
        photos=photos,
        gallery_message=gallery_message,
        active_day=active_day,
        active_is_today=is_today,
        upload_enabled=_upload_enabled(),
    )


@ferie_bp.post(f"/{TRIP_SLUG}/bilder")
def upload_trip_photos():
    configured_code = (os.getenv("FERIE_UPLOAD_CODE") or "").strip()
    submitted_code = (request.form.get("upload_code") or "").strip()
    if not configured_code or not hmac.compare_digest(configured_code, submitted_code):
        flash("Feil opplastingskode.", "danger")
        return redirect(url_for("ferie.south_england_trip") + "#bilder")

    bucket = _s3_bucket()
    if not bucket:
        flash("Bildearkivet er ikke konfigurert.", "warning")
        return redirect(url_for("ferie.south_england_trip") + "#bilder")

    files = [file for file in request.files.getlist("photos") if file and file.filename]
    if not files:
        flash("Velg minst ett bilde.", "warning")
        return redirect(url_for("ferie.south_england_trip") + "#bilder")
    if len(files) > MAX_FILES_PER_UPLOAD:
        flash(f"Du kan laste opp maksimalt {MAX_FILES_PER_UPLOAD} bilder om gangen.", "warning")
        return redirect(url_for("ferie.south_england_trip") + "#bilder")

    caption = (request.form.get("caption") or "").strip()[:180]
    place = (request.form.get("place") or "").strip()[:80]
    client = _s3_client()

    try:
        manifest = _read_manifest(client, bucket)
        uploaded = []
        now = dt.datetime.now(dt.timezone.utc)
        for index, file in enumerate(files):
            raw = file.read(MAX_PHOTO_BYTES + 1)
            image_bytes = _prepare_jpeg(raw)
            photo_id = uuid.uuid4().hex
            key = f"{_s3_prefix()}/photos/{now:%Y/%m/%d}/{now:%Y%m%dT%H%M%S}-{index:02d}-{photo_id}.jpg"
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=image_bytes,
                ContentType="image/jpeg",
                CacheControl="public, max-age=86400",
                Metadata={"trip": TRIP_SLUG},
            )
            uploaded.append(
                {
                    "id": photo_id,
                    "key": key,
                    "caption": caption,
                    "place": place,
                    "uploaded_at": now.isoformat(timespec="seconds"),
                }
            )
        _write_manifest(client, bucket, uploaded + manifest)
        flash(f"{len(uploaded)} bilde(r) er lagt til i feriegalleriet.", "success")
    except ValueError as exc:
        flash(str(exc), "warning")
    except (BotoCoreError, ClientError, json.JSONDecodeError) as exc:
        current_app.logger.exception("Bildeopplasting feilet: %s", exc)
        flash("Bildeopplastingen feilet. Prøv igjen senere.", "danger")

    return redirect(url_for("ferie.south_england_trip") + "#bilder")

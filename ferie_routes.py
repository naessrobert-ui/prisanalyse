# -*- coding: utf-8 -*-
"""Public trip page, family photo upload and trip Q&A for South England 2026."""

from __future__ import annotations

import datetime as dt
import hmac
import io
import json
import os
import threading
import time
import uuid
from urllib.parse import quote
from zoneinfo import ZoneInfo

import anthropic
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Blueprint, current_app, flash, jsonify, redirect, render_template, request, url_for
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
QUESTION_WINDOW_SECONDS = 30 * 60
QUESTION_MAX_PER_WINDOW = 6
QUESTION_MAX_LENGTH = 350
_QUESTION_BUCKETS: dict[str, list[float]] = {}
_QUESTION_LOCK = threading.Lock()


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
            {"label": "Hydro Hotel", "url": "https://www.hydrohotel.com/"},
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
        "summary": "Tidlig løpetur og tog til Brighton, fast leiebilhenting kl. 10 og sightseeing på vei til Rye.",
        "items": [
            {"time": "06:15", "title": "Robert løper til Seaford", "detail": "Omtrent 20–22 kuperte kilometer langs kysten. Skift til tørre klær og ta tog videre til Brighton."},
            {"time": "ca. 08:30", "title": "Helene reiser med bagasjen", "detail": "Tidlig utsjekking og tog fra Eastbourne til Brighton. Avtal konkret møtested ved Dollar."},
            {"time": "10:00", "title": "Hent leiebilen hos Dollar", "detail": "Russell Road, Brighton BN1 2DX. Compact Vauxhall Astra eller tilsvarende, manuell girkasse og aircondition."},
            {"time": "12:00–14:30", "title": "Battle Abbey", "detail": "Slagmarken fra 1066, klosterruinene og portbygningen. Beregn rundt to timer."},
            {"time": "15:00", "title": "Hastings – valgfritt stopp", "detail": "Kort rusletur og kaffe i Old Town. Dropp stoppet dersom dagen har blitt lang."},
            {"time": "17:00–18:00", "title": "Ankomst Rye", "detail": "Innsjekking på The Hope Anchor. Deluxe Double Room med frokost."},
        ],
        "links": [
            {"label": "Løpemål Seaford", "url": _maps_url("Seaford railway station East Sussex")},
            {"label": "Dollar Brighton", "url": _maps_url("Dollar Car Rental Russell Road Brighton BN1 2DX")},
            {"label": "Battle Abbey", "url": "https://www.english-heritage.org.uk/visit/places/1066-battle-of-hastings-abbey-and-battlefield/"},
            {"label": "The Hope Anchor", "url": "https://thehopeanchor.com/"},
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
            {"time": "10:00–11:45", "title": "Bodiam Castle", "detail": "Slott med vollgrav. Kontroller åpningstid live før avreise."},
            {"time": "12:15", "title": "Tilbake til Rye", "detail": "Parker bilen for resten av dagen."},
            {"time": "13:00", "title": "Charles Palmer Wine & Cheese", "detail": "Fire viner og fire oster. Ta taxi begge veier slik at begge kan smake."},
            {"time": "15:00–18:00", "title": "Rye gamleby", "detail": "Mermaid Street, St Mary’s Church, småbutikker og utsikt over Rother-landskapet."},
            {"time": "19:30", "title": "Middag i Rye", "detail": "Bestill bord og avklar cøliaki og krysskontaminering på forhånd."},
        ],
        "links": [
            {"label": "Bodiam Castle", "url": "https://www.nationaltrust.org.uk/visit/sussex/bodiam-castle"},
            {"label": "Charles Palmer", "url": "https://www.charlespalmer-vineyards.co.uk/wine-tastings/"},
            {"label": "Mermaid Street", "url": _maps_url("Mermaid Street Rye")},
        ],
    },
    {
        "date": "2026-07-23",
        "weekday": "Torsdag",
        "place": "Rye → Brighton",
        "stay": "Mercure Brighton Seafront Hotel",
        "summary": "Rolig morgen, retur til Brighton, bagasje på hotellet og bilinnlevering kl. 15.",
        "items": [
            {"time": "08:30", "title": "Frokost og utsjekking", "detail": "Avreise rundt 10 gir god margin til Brighton."},
            {"time": "10:00–13:15", "title": "Kjør mot Brighton", "detail": "Valgfri kort pause i Lewes dersom trafikken og dagsformen tillater det."},
            {"time": "13:30–14:15", "title": "Mercure Brighton Seafront", "detail": "Sett fra dere bagasjen. Innsjekking 14–15 er ønsket, men hotellet har bare lovet å gjøre sitt beste dersom rommet er klart."},
            {"time": "15:00", "title": "Lever leiebilen hos Dollar", "detail": "Samme adresse i Russell Road. Kontoret er åpent til 16:30, men reservasjonen har fast levering kl. 15."},
            {"time": "fra 15:30", "title": "Brighton med familien", "detail": "Balkong, sjøutsikt, The Lanes og middag sammen."},
        ],
        "links": [
            {"label": "Mercure Brighton", "url": "https://www.mercurebrighton.co.uk/"},
            {"label": "Dollar Brighton", "url": _maps_url("Dollar Car Rental Russell Road Brighton BN1 2DX")},
            {"label": "Visit Brighton", "url": "https://www.visitbrighton.com/"},
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

HOTELS = [
    {
        "name": "Hydro Hotel",
        "place": "Eastbourne",
        "dates": "19.–21. juli · 2 netter",
        "official_url": "https://www.hydrohotel.com/",
        "maps_url": _maps_url("Hydro Hotel Eastbourne"),
        "booking": "Hage, oppvarmet utendørsbasseng og frokost.",
        "history": "Hotellet åpnet i 1895. Det ligger høyt over sjøen og ble bygget som et sted for ro, luft og rekreasjon, med private hager, terrasser og vid utsikt mot Den engelske kanal og Beachy Head.",
        "highlights": ["Privat hage", "Utendørsbasseng", "Panoramautsikt", "Glutenfrie retter ved forhåndsvarsel"],
        "source_url": "https://www.hydrohotel.com/history/",
    },
    {
        "name": "The Hope Anchor",
        "place": "Rye",
        "dates": "21.–23. juli · 2 netter",
        "official_url": "https://thehopeanchor.com/",
        "maps_url": _maps_url("The Hope Anchor Rye"),
        "booking": "Deluxe Double Room, 30 m² og frokost inkludert.",
        "history": "Bygningen ble reist omkring 1750 for lokale sjøfolk og skipsbyggere. Hotellet ligger høyt i gamlebyen og er knyttet til Ryes lange sjøfarts- og smuglerhistorie.",
        "highlights": ["Midt i gamlebyen", "Panoramautsikt", "Historisk bygning", "Kort vei til Mermaid Street"],
        "source_url": "https://thehopeanchor.com/",
    },
    {
        "name": "Mercure Brighton Seafront Hotel",
        "place": "Brighton",
        "dates": "23.–24. juli · 1 natt",
        "official_url": "https://www.mercurebrighton.co.uk/",
        "maps_url": _maps_url("Mercure Brighton Seafront Hotel 149 Kings Road Brighton BN1 2PP"),
        "booking": "Rom med balkong og sjøutsikt. Ønsket innsjekking kl. 14–15 er registrert, men avhenger av at rommet er klart.",
        "history": "Det tidligere Norfolk Hotel ble oppført i 1864–65 og er et vernet viktoriansk landemerke ved strandpromenaden. Bygningen har en Regency-inspirert stukkfasade, originale speil og lysekroner.",
        "highlights": ["149 Kings Road", "Balkong og sjøutsikt", "Grade II-vernet", "Rett ved strandpromenaden"],
        "source_url": "https://www.mercurebrighton.co.uk/hotel-highlights/",
    },
]

PLACES = [
    {
        "name": "Eastbourne",
        "period": "Viktoriansk badeby",
        "intro": "Eastbourne utviklet seg raskt som ferie- og kursted på 1800-tallet, men områdets historie går langt tilbake før den elegante strandpromenaden og de store hotellene.",
        "fact": "Byens kulturarv spenner fra forhistoriske funn og romerske spor til Napoleons-forsvar, viktoriansk arkitektur og kunstmiljøet rundt South Downs.",
        "url": "https://www.visiteastbourne.com/explore/heritage",
    },
    {
        "name": "Seven Sisters",
        "period": "Krittklipper og naturreservat",
        "intro": "De bølgende, hvite klippene ligger mellom Seaford og Eastbourne og er en del av Sussex Heritage Coast og South Downs.",
        "fact": "Klippene er levende geologi: kritt brytes gradvis ned av vær og sjø. Derfor må man holde god avstand til kanten, også når bakken ser stabil ut.",
        "url": "https://www.sevensisters.org.uk/about/",
    },
    {
        "name": "Battle Abbey",
        "period": "1066 og normannernes England",
        "intro": "Slaget ved Hastings ble utkjempet 14. oktober 1066. William Erobrerens seier over Harald Godwinson endret England politisk, språklig og kulturelt.",
        "fact": "Klosteret ble grunnlagt som et minnesmerke etter slaget. Høyalteret skal ha blitt plassert på stedet der kong Harald falt.",
        "url": "https://www.english-heritage.org.uk/visit/places/1066-battle-of-hastings-abbey-and-battlefield/history-and-stories/history/",
    },
    {
        "name": "Rye",
        "period": "Havn, Cinque Ports og smuglere",
        "intro": "Rye ligger nå inne i landet, men var tidligere en viktig havneby. Middelaldergater, bindingsverkshus og utsikt over myrområdene viser hvordan landskapet og sjøen har formet byen.",
        "fact": "Byen var knyttet til Cinque Ports og har en sterk smuglerhistorie. St Mary’s Church, Mermaid Street og Ypres Tower er gode steder å forstå historien.",
        "url": "https://visitrye.co.uk/rye-history",
    },
    {
        "name": "Bodiam Castle",
        "period": "Slutten av 1300-tallet",
        "intro": "Bodiam er et av Englands mest fotogene slott, bygget med firkantet plan, hjørnetårn og en bred vollgrav.",
        "fact": "Sir Edward Dalyngrigge fikk tillatelse til å befeste eiendommen i 1385. Slottet var både forsvarsverk, statussymbol og komfortabel bolig.",
        "url": "https://www.nationaltrust.org.uk/visit/sussex/bodiam-castle/history-of-bodiam-castle",
    },
    {
        "name": "Brighton",
        "period": "Regency, kongehus og moderne kystby",
        "intro": "Brighton ble forvandlet da den senere George IV forelsket seg i byen på slutten av 1700-tallet. Regency-arkitekturen og Royal Pavilion ble symboler på byens nye rolle som fornøyelsessted.",
        "fact": "I dag kombinerer Brighton historiske plasser og pirer med kunst, musikk, uavhengige butikker og en tydelig åpen og kreativ identitet.",
        "url": "https://www.visitbrighton.com/about",
    },
]

BOOKINGS = [
    {"category": "Fly", "name": "Gatwick", "dates": "Søn. 19. juli kl. 20:20", "status": "Bekreftet", "note": "Flynummer kan legges inn senere.", "url": "https://www.gatwickairport.com/"},
    {"category": "Hotell", "name": "Hydro Hotel, Eastbourne", "dates": "19.–21. juli · 2 netter", "status": "Bestilt", "note": "Hage og oppvarmet utendørsbasseng.", "url": "https://www.hydrohotel.com/"},
    {"category": "Hotell", "name": "The Hope Anchor, Rye", "dates": "21.–23. juli · 2 netter", "status": "Bestilt", "note": "Deluxe Double Room, 30 m², frokost inkludert.", "url": "https://thehopeanchor.com/"},
    {"category": "Bil", "name": "Dollar, Brighton", "dates": "Henting tir. 21. juli kl. 10 · levering tor. 23. juli kl. 15", "status": "Bestilt", "note": "Russell Road, BN1 2DX. Vauxhall Astra eller tilsvarende, kompakt, manuell og aircondition. Kontortid 08:00–16:30.", "url": _maps_url("Dollar Car Rental Russell Road Brighton BN1 2DX")},
    {"category": "Hotell", "name": "Mercure Brighton Seafront Hotel", "dates": "23.–24. juli · 1 natt", "status": "Bestilt", "note": "149 Kings Road. Balkong og sjøutsikt; innsjekking 14–15 er ønsket, ikke garantert.", "url": "https://www.mercurebrighton.co.uk/"},
    {"category": "Opplevelse", "name": "Charles Palmer Vineyards", "dates": "Ons. 22. juli kl. 13:00", "status": "Kontroller", "note": "Wine & Cheese Experience må være bekreftet før avreise.", "url": "https://www.charlespalmer-vineyards.co.uk/wine-tastings/"},
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


def _ask_enabled() -> bool:
    value = (os.getenv("FERIE_CHAT_ENABLED") or "1").strip().lower()
    return bool(os.getenv("ANTHROPIC_API_KEY")) and value not in {"0", "false", "no", "off"}


def _question_allowed(ip: str) -> bool:
    now = time.monotonic()
    cutoff = now - QUESTION_WINDOW_SECONDS
    with _QUESTION_LOCK:
        hits = [hit for hit in _QUESTION_BUCKETS.get(ip, []) if hit >= cutoff]
        if len(hits) >= QUESTION_MAX_PER_WINDOW:
            _QUESTION_BUCKETS[ip] = hits
            return False
        hits.append(now)
        _QUESTION_BUCKETS[ip] = hits
        return True


def _question_context() -> str:
    compact = {
        "reiseplan": ITINERARY,
        "hoteller": HOTELS,
        "steder_og_historie": PLACES,
        "reservasjoner": BOOKINGS,
        "viktige_hensyn": [
            "Robert har cøliaki og må ha glutenfri mat uten krysskontaminering.",
            "Leiebilen hentes hos Dollar i Brighton tirsdag kl. 10 og leveres samme sted torsdag kl. 15.",
            "Vinsmaking bør gjennomføres med taxi, ikke kjøring etterpå.",
            "Siden viser ikke bookingnumre eller betalingsinformasjon.",
        ],
    }
    return json.dumps(compact, ensure_ascii=False)


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
    return next(item for day, item in parsed if day > today), False


@ferie_bp.route(f"/{TRIP_SLUG}/")
def south_england_trip():
    photos, gallery_message = _list_photos()
    active_day, is_today = _current_day()
    return render_template(
        "ferie_sor_england_2026.html",
        trip_title=TRIP_TITLE,
        itinerary=ITINERARY,
        hotels=HOTELS,
        places=PLACES,
        bookings=BOOKINGS,
        photos=photos,
        gallery_message=gallery_message,
        active_day=active_day,
        active_is_today=is_today,
        upload_enabled=_upload_enabled(),
        ask_enabled=_ask_enabled(),
    )


@ferie_bp.post(f"/{TRIP_SLUG}/sporsmal")
def ask_about_trip():
    if not _ask_enabled():
        return jsonify({"error": "Reiseassistenten er ikke aktivert ennå."}), 503

    ip = (request.headers.get("X-Forwarded-For", "") or request.remote_addr or "unknown").split(",")[0].strip()
    if not _question_allowed(ip):
        return jsonify({"error": "For mange spørsmål på kort tid. Vent litt og prøv igjen."}), 429

    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question") or "").strip()
    if len(question) < 3:
        return jsonify({"error": "Skriv et litt mer utfyllende spørsmål."}), 400
    if len(question) > QUESTION_MAX_LENGTH:
        return jsonify({"error": f"Spørsmålet kan være maksimalt {QUESTION_MAX_LENGTH} tegn."}), 400

    system_prompt = (
        "Du er en kortfattet norsk reiseassistent for Robert og Helenes Sør-England-tur i juli 2026. "
        "Svar først og fremst ut fra reiseinformasjonen du får. Ikke finn på bookingnumre, hotellnavn eller bekreftelser. "
        "Når brukeren spør om åpningstider, tog, vær eller andre opplysninger som kan endres, si tydelig at dette må kontrolleres live. "
        "Ta alltid hensyn til cøliaki når mat er relevant. Svar vennlig og konkret, normalt på 3–8 setninger."
    )
    user_prompt = f"REISEINFORMASJON:\n{_question_context()}\n\nSPØRSMÅL:\n{question}"

    try:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        response = client.messages.create(
            model=(os.getenv("FERIE_CHAT_MODEL") or "claude-sonnet-4-6").strip(),
            max_tokens=500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        answer = "".join(getattr(block, "text", "") for block in response.content).strip()
        if not answer:
            raise ValueError("Tomt svar fra modellen")
        return jsonify({"answer": answer})
    except Exception as exc:
        current_app.logger.warning("Reiseassistenten feilet: %s", exc)
        return jsonify({"error": "Kunne ikke svare akkurat nå. Prøv igjen senere."}), 503


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

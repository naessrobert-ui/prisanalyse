"""
scripts/kupp_import_pushover.py – Bakfyll kupp-loggen fra Pushover-varsler
=========================================================================
Kupp-vakten begynte først nylig å logge egenskapene til hver varslede bil
(scripts/kupp_vakt.py -> calc/bil/kupp_vakt_logg.json). Varsler sendt før det
finnes bare som Pushover-meldinger på telefonen. Meldingsteksten inneholder
heldigvis det meste vi trenger: FinnKode (i URL-en), merke, modell, år, km,
sted, pris, rabatt % (og kr) og forventet pris.

Dette skriptet leser slik tekst (limt inn fra Pushover-appen) og skriver den
inn i den samme S3-loggen, slik at de gamle bilene også kommer med i
`scripts/kupp_analyse.py`. Eksisterende logg-rader røres ikke – live-loggen har
alltid forrang (den har i tillegg drivstoff/hurtigpris o.l. som meldingen ikke
viser).

Slik får du teksten ut av iPhone-appen
--------------------------------------
Åpne Pushover -> hver «🚗 Kupp-vakt»-melding, marker og kopier teksten, og lim
inn i én tekstfil (rekkefølge spiller ingen rolle). Vil du ha riktig dato på
bilene, sett en datolinje foran gruppene fra samme dag:

    == 2026-08-08 ==
    Tesla Model 3 2021, 45 000 km – Bergen
    250 000 kr (-19% / -60 000 kr mot 310 000)
    https://www.finn.no/mobility/item/123456789

    == 2026-08-09 ==
    ...

Uten datolinjer brukes --flagget (default: i dag). Overskrifts- og
«… +N flere»-linjer ignoreres automatisk.

Kjøring
-------
    python -m scripts.kupp_import_pushover meldinger.txt            # skriv til S3
    python -m scripts.kupp_import_pushover meldinger.txt --dry-run  # bare vis
    python -m scripts.kupp_import_pushover meldinger.txt --flagget 2026-08-08
    cat meldinger.txt | python -m scripts.kupp_import_pushover -    # fra stdin
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.kupp_vakt import S3_BUCKET, LOGG_KEY, LOGG_TTL_DAYS, _s3  # noqa: E402

# Linje 1:  "Merke Modell ... ÅR, KM km[ – Sted]"
_RE_TITTEL = re.compile(
    r"^(?P<navn>.+?)\s+(?P<aar>(?:19|20)\d{2}),\s*"
    r"(?P<km>[\d\s ]+?)\s*km"
    r"(?:\s*[–\-]\s*(?P<sted>.+))?$"
)
# Linje 2:  "PRIS kr (-RAB% [/ -RAB_KR kr] mot FORVENTET)"
_RE_PRIS = re.compile(
    r"^(?P<pris>[\d\s ]+)\s*kr\s*\(\s*-?(?P<pct>[\d.,]+)\s*%"
    r"(?:\s*/\s*-?(?P<kr>[\d\s ]+)\s*kr)?\s*mot\s*(?P<forv>[\d\s ]+)\)"
)
_RE_URL = re.compile(r"/(?:mobility/)?item/(\d+)")
_RE_DATO = re.compile(r"(\d{4})[-/.](\d{2})[-/.](\d{2})")


def _tall(s):
    if s is None:
        return None
    d = re.sub(r"[^\d]", "", str(s))
    return float(d) if d else None


def _flagget_iso(dato_str: str | None, fallback: str) -> str:
    d = dato_str or fallback
    m = _RE_DATO.search(d or "")
    if m:
        y, mo, da = map(int, m.groups())
        return datetime(y, mo, da, 12, 0, tzinfo=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def parse(tekst: str, default_dato: str | None) -> dict:
    """Returner {FinnKode: record}. Blokk = tittel, prislinje, url i rekkefølge;
    url-linja avslutter blokken. Datolinjer (== 2026-08-08 ==) setter tidspunkt
    for påfølgende biler."""
    fallback = default_dato or datetime.now(timezone.utc).date().isoformat()
    dato = None
    buffer: list[str] = []
    ut: dict = {}
    selger = os.getenv("KUPP_SELGER", "privat").strip() or "alle"
    fylke = os.getenv("KUPP_FYLKE", "").strip() or "hele landet"

    for rå in tekst.splitlines():
        linje = rå.strip().replace(" ", " ")
        if not linje:
            continue

        # Datolinje?  == 2026-08-08 ==  /  # 2026-08-08  /  bare en dato
        if _RE_DATO.fullmatch(linje.strip("=# ").strip()) or (
            linje.startswith(("==", "#")) and _RE_DATO.search(linje)
        ):
            dato = _RE_DATO.search(linje).group(0)
            buffer = []
            continue

        # Overskrift / hale – hopp over
        if linje.startswith("🚗") or "kupp-vakt" in linje.lower() or linje.startswith("…") or linje.startswith("..."):
            continue

        m_url = _RE_URL.search(linje)
        if m_url:
            fk = m_url.group(1)
            tittel = buffer[-2] if len(buffer) >= 2 else ""
            prislinje = buffer[-1] if len(buffer) >= 1 else ""
            buffer = []
            rec = _bygg_record(fk, tittel, prislinje, _flagget_iso(dato, fallback), selger, fylke)
            if rec:
                ut[fk] = rec
            continue

        buffer.append(linje)

    return ut


def _bygg_record(fk, tittel, prislinje, flagget, selger, fylke):
    mt = _RE_TITTEL.match(tittel or "")
    mp = _RE_PRIS.match(prislinje or "")
    if not fk:
        return None
    navn = (mt.group("navn").strip() if mt else "").split(maxsplit=1)
    merke = navn[0] if navn else None
    modell = navn[1] if len(navn) > 1 else None
    return {
        "flagget": flagget,
        "Merke": merke,
        "Modell": modell,
        "Årstall": int(mt.group("aar")) if mt else None,
        "Kjørelengde": _tall(mt.group("km")) if mt else None,
        "Drivstoff": None,
        "Hjuldrift": None,
        "sted": (mt.group("sted").strip() if mt and mt.group("sted") else None),
        "pris": _tall(mp.group("pris")) if mp else None,
        "forventet_pris": _tall(mp.group("forv")) if mp else None,
        "hurtigpris": None,
        "innbyttepris": None,
        "rabatt_pct": (float(mp.group("pct").replace(",", ".")) if mp else None),
        "rabatt_kr": (_tall(mp.group("kr")) if mp and mp.group("kr") else None),
        "modell_nivaa": None,
        "selger_filter": selger,
        "fylke_filter": fylke,
        "url": f"https://www.finn.no/mobility/item/{fk}",
        "kilde": "pushover-import",
    }


def _skriv_s3(s3, nye: dict) -> int:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=LOGG_KEY)
        logg = json.loads(obj["Body"].read().decode("utf-8"))
        if not isinstance(logg, dict):
            logg = {}
    except Exception:
        logg = {}

    lagt_til = 0
    for fk, rec in nye.items():
        if fk not in logg:  # live-logg har forrang
            logg[fk] = rec
            lagt_til += 1

    grense = datetime.now(timezone.utc) - timedelta(days=LOGG_TTL_DAYS)
    renset = {}
    for fk, rec in logg.items():
        try:
            if datetime.fromisoformat(rec.get("flagget", "")) >= grense:
                renset[fk] = rec
        except Exception:
            renset[fk] = rec

    s3.put_object(
        Bucket=S3_BUCKET, Key=LOGG_KEY,
        Body=json.dumps(renset, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return lagt_til, len(renset)


def main():
    ap = argparse.ArgumentParser(description="Bakfyll kupp-loggen fra Pushover-tekst")
    ap.add_argument("fil", help="Tekstfil med innlimte meldinger, eller - for stdin")
    ap.add_argument("--flagget", help="Dato (YYYY-MM-DD) for biler uten datolinje")
    ap.add_argument("--dry-run", action="store_true", help="Vis hva som ville blitt lagt til")
    args = ap.parse_args()

    tekst = sys.stdin.read() if args.fil == "-" else Path(args.fil).read_text(encoding="utf-8")
    nye = parse(tekst, args.flagget)
    print(f"[import] Tolket {len(nye)} biler fra teksten")
    for fk, r in nye.items():
        print(f"  {fk}: {r['Merke'] or '?'} {r['Modell'] or ''} "
              f"{r['Årstall'] or '?'} | {int(r['pris']) if r['pris'] else '?'} kr "
              f"(-{r['rabatt_pct'] or '?'}%) | flagget {r['flagget'][:10]}")

    if args.dry_run:
        print("[import] --dry-run: skrev ingenting.")
        return
    if not nye:
        print("[import] Ingenting å skrive.")
        return

    lagt_til, total = _skriv_s3(_s3(), nye)
    print(f"[import] La til {lagt_til} nye biler (loggen har nå {total}). "
          "Kjør `python -m scripts.kupp_analyse` for oppdatert rapport.")


if __name__ == "__main__":
    main()

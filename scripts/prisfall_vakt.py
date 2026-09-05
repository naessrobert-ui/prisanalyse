"""Prisfallvakt for rene elbiler, kjørt etter Bilradars eksisterende scoring.

Leser hele bilradar_aktive.parquet, ikke topplisten eller bare nye annonser.
Ingen nye FINN-oppslag. Egen S3-state; første kjøring lagrer kun grunnpriser.
Se docs/prisfall_vakt.md for drift, terskler og begrensninger.
"""
from __future__ import annotations

import argparse
import copy
import io
import json
import math
import os
from datetime import datetime, timezone

import pandas as pd
from botocore.exceptions import ClientError

from scripts import kupp_vakt as kupp

STATE_KEY = os.getenv("PRISFALL_STATE_KEY", "calc/bil/prisfall_vakt_state.json")
INPUT_KEY = os.getenv("PRISFALL_INPUT_KEY", "calc/bil/bilradar_aktive.parquet")
MIN_KR = float(os.getenv("PRISFALL_MIN_KR", "10000"))
MIN_PCT = float(os.getenv("PRISFALL_MIN_PCT", "3"))
MAX_VARSLER = int(os.getenv("PRISFALL_MAX_VARSLER", "40"))
MAX_AGE_DAYS = 2
STATE_DAYS = 120


def _tekst(value) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def _tall(value) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def _tid(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


def _kode(value) -> str:
    number = _tall(value)
    return str(int(number)) if number and number > 0 and number.is_integer() else ""


def _fylkekode(value) -> str:
    name = _tekst(value).lower()
    return kupp.FYLKE_LOCATION.get(name, name)


def _varselrad(row: dict) -> dict:
    """Adapter batch-kolonner til kuppvaktens terskler og meldingsformat."""
    out = dict(row)
    for dest, src in (("Merke", "Produsent"), ("Årstall", "årstall"),
                      ("Kjørelengde", "kjørelengde"), ("Drivstoff", "drivstoff")):
        out[dest] = row.get(src)
    out["FinnKode"] = _kode(row.get("FinnKode"))
    out["Pris"] = float(row["salgspris"])
    out["url"] = kupp.FINN_ITEM_URL.format(out["FinnKode"])
    for col in ("Merke", "Modell", "Drivstoff", "sted"):
        out[col] = _tekst(out.get(col))
    return out


def _attraktiv(row: dict) -> bool:
    """Samme pris-/merke-/geografikrav som kuppvakten, uten å scrape fylker."""
    code = _fylkekode(row.get("fylke"))
    if kupp.LOCATION_CODES and code not in kupp.LOCATION_CODES:
        return False
    if kupp.STED_FILTER and not any(s in row["sted"].lower() for s in kupp.STED_FILTER):
        return False
    fk = row["FinnKode"]
    hjem = None
    if kupp.HJEMFYLKE_KODE and kupp.UTENFOR_TILLEGG_PP > 0:
        hjem = {fk} if code == kupp.HJEMFYLKE_KODE else set()
    nabo = {fk} if code in kupp.NABO_KODER else set()
    return kupp._er_kupp(row, kupp._terskel_delta(row, hjem, nabo))


def finn_prisfall(df: pd.DataFrame, state: dict | None, now=None,
                  *, seed=False, min_kr=MIN_KR, min_pct=MIN_PCT):
    """Returner neste state og attraktive prisfall uten I/O eller varsling.

    Nye annonser etablerer en grunnpris. En ventende hendelse beholdes ved
    manglende verdsettelse eller mislykket sending, men fjernes ved ny pris.
    Pris_ny/Pris sammenlignes aldri: Pris er annonsens opprinnelige pris.
    """
    if min_kr <= 0 or min_pct <= 0:
        raise ValueError("Prisfalltersklene må være positive")
    required = {"FinnKode", "drivstoff", "Solgt", "salgspris", "Dato_ny",
                "forventet_pris"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mangler kolonner i Bilradar-data: {sorted(missing)}")
    now = _tid(now if now is not None else datetime.now(timezone.utc))
    initial = state is None or seed
    result = {"version": 1, "cars": {}} if initial else copy.deepcopy(state)
    cars = result["cars"]
    rows = df.copy()
    rows["_observed"] = pd.to_datetime(rows["Dato_ny"], utc=True, errors="coerce")
    rows["_fk"] = rows["FinnKode"].map(_kode)
    rows = rows.sort_values("_observed", na_position="first").drop_duplicates("_fk", keep="last")
    candidates = []
    for raw in rows.to_dict("records"):
        fk = raw["_fk"]
        observed = raw["_observed"]
        if not fk or pd.isna(observed):
            continue
        previous = cars.get(fk)
        if previous and observed < _tid(previous["observed_at"]):
            continue  # eldre snapshot må ikke spole tilbake grunnpris eller varsler
        if (_tekst(raw.get("Solgt")).upper() != "NEI"
                or kupp._norm_driv(_tekst(raw.get("drivstoff"))) != "elektrisk"):
            if previous:
                previous.pop("pending", None)
            continue
        if observed < now - pd.Timedelta(days=MAX_AGE_DAYS) or observed > now:
            continue
        price = _tall(raw.get("salgspris"))
        if price is None or price < 1500:
            continue  # tom/0/1/NaN/inf er ikke en ny grunnpris
        price = round(price, 2)
        record = previous or {"price": price, "alerted_prices": []}
        old_price = record["price"]
        if price != old_price:
            record.pop("pending", None)
            drop = old_price - price
            if (drop > 0 and (drop >= min_kr or drop / old_price * 100 >= min_pct)
                    and price not in record["alerted_prices"]):
                record["pending"] = {"old_price": old_price, "new_price": price,
                                     "observed_at": observed.isoformat()}
        record.update(price=price, observed_at=observed.isoformat())
        cars[fk] = record
        pending = record.get("pending")
        if not pending or initial:
            continue
        if _tid(pending["observed_at"]) < now - pd.Timedelta(days=MAX_AGE_DAYS):
            record.pop("pending", None)
            continue
        expected = _tall(raw.get("forventet_pris"))
        if expected is None or expected <= 0:
            continue  # ny scoring kan lykkes ved neste kjøring
        row = _varselrad(raw)
        # Regn rabatt fra dagens pris og verdsettelse, aldri fra gamle rabattfelt.
        row["forventet_pris"] = expected
        row["rabatt_kr"] = expected - price
        row["rabatt_pct"] = (expected - price) / expected * 100
        if not _attraktiv(row):
            record.pop("pending", None)
            continue
        row["pris_for"] = pending["old_price"]
        row["prisfall_kr"] = pending["old_price"] - price
        row["prisfall_pct"] = row["prisfall_kr"] / pending["old_price"] * 100
        candidates.append(row)
    # Historikk om nylig fraværende biler beholdes for å hindre gjentatte varsler.
    result["cars"] = {fk: r for fk, r in cars.items()
                      if _tid(r["observed_at"]) >= now - pd.Timedelta(days=STATE_DAYS)}
    result["updated_at"] = now.isoformat()
    candidates.sort(key=lambda row: row["rabatt_kr"], reverse=True)
    return result, candidates


def last_state(s3):
    try:
        obj = s3.get_object(Bucket=kupp.S3_BUCKET, Key=STATE_KEY)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise  # tilgangs-/nettfeil skal aldri tolkes som første kjøring
    state = json.loads(obj["Body"].read())
    if state.get("version") != 1 or not isinstance(state.get("cars"), dict):
        raise ValueError("Ukjent format på prisfall-state; avbryter uten overskriving")
    for r in state["cars"].values():
        if (not isinstance(r, dict) or (_tall(r.get("price")) or 0) <= 0
                or pd.isna(_tid(r.get("observed_at")))
                or not isinstance(r.get("alerted_prices"), list)):
            raise ValueError("Ugyldig prisfall-state; avbryter uten overskriving")
    return state


def lagre_state(s3, state):
    s3.put_object(Bucket=kupp.S3_BUCKET, Key=STATE_KEY,
                  Body=json.dumps(state, ensure_ascii=False, allow_nan=False).encode("utf-8"),
                  ContentType="application/json")


def _melding(row):
    def kr(value):
        return f"{value:,.0f}".replace(",", " ")
    name = f"{row['Merke']} {row['Modell']}"[:140]
    return (
        f"{name} {_tekst(row.get('Årstall'))} · {row['sted'][:80]}\n"
        f"Pris: {kr(row['pris_for'])} → {kr(row['Pris'])} kr\n"
        f"Ned {kr(row['prisfall_kr'])} kr ({row['prisfall_pct']:.1f} %)\n"
        f"Beregnet verdi: {kr(row['forventet_pris'])} kr\n"
        f"{row['rabatt_pct']:.1f} % under beregnet verdi\n"
        f"{row['url']}"
    )


def kjor(input_path=None, *, seed=False, dry_run=False, s3=None, now=None):
    if MAX_VARSLER < 1:
        raise ValueError("PRISFALL_MAX_VARSLER må være minst 1")
    s3 = s3 if s3 is not None else kupp._s3()
    state = last_state(s3)
    if input_path:
        df = pd.read_parquet(input_path)
    else:
        obj = s3.get_object(Bucket=kupp.S3_BUCKET, Key=INPUT_KEY)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    next_state, candidates = finn_prisfall(df, state, now, seed=seed)
    print(f"[prisfall_vakt] {len(next_state['cars'])} elbiler i state; "
          f"{len(candidates)} attraktive prisfall")
    if state is None or seed:
        print("[prisfall_vakt] Grunnpriser lagret uten varsling" if not dry_run
              else "[prisfall_vakt] Ville etablert grunnpriser uten varsling")
    if dry_run:
        for row in candidates[:MAX_VARSLER]:
            print(_melding(row))
        return 0
    # Lagre ventende hendelser før sending. Feilet state-skriving stopper varsler.
    lagre_state(s3, next_state)
    for row in candidates[:MAX_VARSLER]:
        ok = kupp._send_pushover([row], melding=_melding(row),
                                 tittel="🚘 Prisfall: attraktiv elbil")
        if not ok:
            print("[prisfall_vakt] Varsel ikke sendt; beholdes til neste kjøring")
            return 1
        record = next_state["cars"][row["FinnKode"]]
        record["alerted_prices"].append(row["Pris"])
        record.pop("pending", None)
        lagre_state(s3, next_state)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Lokal bilradar_aktive.parquet; ellers S3")
    parser.add_argument("--seed", action="store_true", help="Nullstill grunnpriser uten varsler")
    parser.add_argument("--dry-run", action="store_true", help="Ingen sending eller lagring")
    args = parser.parse_args()
    raise SystemExit(kjor(args.input, seed=args.seed, dry_run=args.dry_run))


if __name__ == "__main__":
    main()

"""
scripts/favoritt_spor.py – favoritt-sporing og «🔥 Populær»-varsel
==================================================================
Kalles fra kupp_vakt.kjor() hver kjøring. Nye kandidater (etter kuppvaktens
filtre) legges i en sporingsliste i S3, og antall favoritter måles:

  * hver kjøring (≈ hvert 10. min) den første timen – for populær-varselet,
  * én gang etter ≈ 3 t og ≈ 24 t – til etteranalyse.

Populær-varsel: en bil som får mange favoritter raskt kan være underpriset på
noe modellen ikke ser (utstyr, stand, farge) og går gjerne lynraskt. Regelen
er uavhengig av rabattkravet; rabatten vises likevel i varselet.

Alder måles fra første gang kuppvakten så annonsen (0–10 min etter
publisering). Kom annonsen etter et opphold i kjøringene (f.eks. natt), er
alderen ukjent – den spores, men gir ikke populær-varsel.

Gamle annonser som publiseres på nytt/løftes havner også øverst i «nyeste».
FINN-koder deles ut fortløpende, så vi lagrer høyeste kode per kjøring. For
populær-varsel må koden være høyere enn det vi så ved en måling i løpet av
de siste KUPP_POPULAER_KODE_MIN minuttene – da er annonsen garantert lagt ut
innenfor det vinduet. Uten slik måling (første time etter natta) varsles ikke.

Ferdige spor flyttes til en logg (S3) for å kalibrere nivåene senere.

Env:
    KUPP_POPULAER_REGLER – "favoritter:minutter,..." (default "10:10,30:60"):
                           minst 10 innen 10 min ELLER 30 innen 60 min.
                           Tom = ingen populær-varsler (sporing fortsetter).
    KUPP_FAV_MAKS_KALL   – maks favoritt-oppslag per kjøring (default 150).
    KUPP_POPULAER_MAKS   – maks populær-varsler per kjøring (default 10).
    KUPP_POPULAER_KODE_MIN – maks alder (min) ut fra FINN-koden (default 60).
    KUPP_FAVORITTER=0    – slår av både sporing og varsler.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from scripts import kupp_vakt as kupp

SPOR_KEY = os.getenv("KUPP_FAV_SPOR_KEY", "calc/bil/kupp_favoritt_spor.json")
LOGG_KEY = os.getenv("KUPP_FAV_LOGG_KEY", "calc/bil/kupp_favoritt_logg.json")
MAKS_KALL = int(os.getenv("KUPP_FAV_MAKS_KALL", "150") or 150)
MAKS_VARSLER = int(os.getenv("KUPP_POPULAER_MAKS", "10") or 10)
MAALEPUNKT_MIN = (180, 1440)       # ekstra målinger etter ≈ 3 t og ≈ 24 t
SPOR_MAKS_MIN = 30 * 60            # gi opp sporet etter 30 t
OPPHOLD_MIN = 25                   # lengre siden forrige kjøring = ukjent alder
KODE_MAKS_MIN = int(os.getenv("KUPP_POPULAER_KODE_MIN", "60") or 60)
KODE_HISTORIKK_TIMER = 48         # rikelig for KUPP_MAKS_ANNONSEALDER_T


def parse_regler(spec: str) -> list[tuple[int, int]]:
    """ "10:10,30:60" -> [(10, 10), (30, 60)] (favoritter, maks minutter)."""
    regler = []
    for del_ in (spec or "").split(","):
        del_ = del_.strip()
        if not del_:
            continue
        fav, minutter = del_.split(":")
        regler.append((int(fav), int(minutter)))
    return regler


REGLER = parse_regler(os.getenv("KUPP_POPULAER_REGLER", "10:10,30:60"))


def _tid(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _alder_min(post: dict, naa: datetime) -> float:
    return (naa - _tid(post["forst_sett"])).total_seconds() / 60


def populaer_regel(fav, alder_min: float, regler) -> tuple[int, int] | None:
    """Første regel som er oppfylt (minst N favoritter innen M minutter)."""
    if fav is None:
        return None
    for n, m in regler:
        if fav >= n and alder_min <= m:
            return (n, m)
    return None


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _txt(v):
    return (v.strip() or None) if isinstance(v, str) else None


def ny_post(b: dict, naa: str, alder_kjent: bool) -> dict:
    """Egenskaper vi vil ha med i varsel og logg (JSON-trygge typer)."""
    aar = _num(b.get("Årstall"))
    return {
        "forst_sett": naa,
        "alder_kjent": alder_kjent,
        "Merke": _txt(b.get("Merke")),
        "Modell": _txt(b.get("Modell")),
        "Årstall": int(aar) if aar is not None else None,
        "Kjørelengde": _num(b.get("Kjørelengde")),
        "Drivstoff": _txt(b.get("Drivstoff")),
        "sted": _txt(b.get("sted")),
        "Pris": _num(b.get("Pris")),
        "forventet_pris": _num(b.get("forventet_pris")),
        "rabatt_pct": _num(b.get("rabatt_pct")),
        "rabatt_kr": _num(b.get("rabatt_kr")),
        "url": _txt(b.get("url")) or kupp.FINN_ITEM_URL.format(b.get("FinnKode")),
        "maalinger": [],
        "populaer_varslet": False,
        "kupp": False,
    }


def _kode(fk) -> int | None:
    try:
        return int(str(fk))
    except (TypeError, ValueError):
        return None


def kode_fersk(fk, historikk: list, naa: datetime,
               maks_min: int = KODE_MAKS_MIN) -> bool | None:
    """Er FINN-koden nyere enn alt vi så for mer enn maks_min minutter siden?
    None = ukjent (for kort historikk eller ugyldig kode)."""
    kode = _kode(fk)
    grense = naa - timedelta(minutes=maks_min)
    gamle = [m for t, m in historikk if _tid(t) <= grense]
    if kode is None or not gamle:
        return None
    return kode > max(gamle)


def kode_sikkert_ny(fk, historikk: list, naa: datetime,
                    maks_min: int = KODE_MAKS_MIN) -> bool | None:
    """Er annonsen garantert opprettet de siste maks_min minuttene?

    True krever en måling *innenfor* vinduet der koden er høyere enn den
    høyeste koden vi så da. Uten måling i vinduet (f.eks. første time etter
    nattopphold) er svaret None: vi kan ikke være sikre. Strengere enn
    kode_fersk, som sammenligner mot siste måling *eldre* enn vinduet – etter
    natta er den fra kvelden før, og da passerer alt som ble lagt ut i natt."""
    kode = _kode(fk)
    grense = naa - timedelta(minutes=maks_min)
    i_vinduet = [m for t, m in historikk if grense <= _tid(t) < naa]
    if kode is None or not i_vinduet:
        return None
    return kode > min(i_vinduet)


def _vindu_min(regler) -> int:
    return max((m for _, m in regler), default=0)


def skal_maales(post: dict, naa: datetime, regler) -> bool:
    alder = _alder_min(post, naa)
    if (alder <= _vindu_min(regler) and post.get("alder_kjent")
            and not post.get("populaer_varslet") and not post.get("kupp")):
        return True
    if not post["maalinger"]:
        return True  # alltid minst én måling (også ved ukjent alder)
    for mp in MAALEPUNKT_MIN:
        if alder >= mp and not any(m["alder_min"] >= mp for m in post["maalinger"]):
            return True
    return False


def _ferdig(post: dict, naa: datetime) -> bool:
    return (_alder_min(post, naa) >= SPOR_MAKS_MIN
            or any(m["alder_min"] >= MAALEPUNKT_MIN[-1] for m in post["maalinger"]))


def oppdater(spor: dict, nye: list[dict], naa: datetime, hent, *,
             regler=REGLER, maks_kall: int = MAKS_KALL,
             kupp_koder=(), forhaandsmaalt: dict | None = None,
             maks_kode: int | None = None):
    """Ren logikk (uten S3/varsling). hent(finnkode) -> int|None.
    maks_kode = høyeste FINN-kode i denne kjøringens søk (alle annonser).

    Returnerer (spor, populaere, ferdige) der populaere er poster som skal
    varsles nå og ferdige er poster som skal flyttes til loggen."""
    forhaandsmaalt = forhaandsmaalt or {}
    biler = spor.setdefault("biler", {})
    sist = spor.get("sist_kjort")
    alder_kjent = bool(sist) and (naa - _tid(sist)) <= timedelta(minutes=OPPHOLD_MIN)
    naa_s = naa.isoformat()
    historikk = spor.get("kode_historikk", [])

    for b in nye:
        fk = str(b.get("FinnKode") or "")
        if fk and fk not in biler:
            fersk = kode_sikkert_ny(fk, historikk, naa)
            biler[fk] = ny_post(b, naa_s, alder_kjent and fersk is True)
            biler[fk]["kode_fersk"] = fersk
    for fk in kupp_koder:
        if fk in biler:
            biler[fk]["kupp"] = True

    # Prioritet: populær-vinduet (yngst først), deretter 3 t/24 t-målinger.
    koe = [fk for fk, p in biler.items() if skal_maales(p, naa, regler)]
    koe.sort(key=lambda fk: (_alder_min(biler[fk], naa) > _vindu_min(regler),
                             _alder_min(biler[fk], naa)))
    kall = 0
    populaere = []
    for fk in koe:
        post = biler[fk]
        if fk in forhaandsmaalt:
            fav = forhaandsmaalt[fk]
        elif kall < maks_kall:
            fav = hent(fk)
            kall += 1
        else:
            continue
        if fav is None:
            continue
        alder = round(_alder_min(post, naa), 1)
        post["maalinger"].append({"t": naa_s, "alder_min": alder, "fav": fav})
        regel = populaer_regel(fav, alder, regler)
        if (regel and post["alder_kjent"] and not post["populaer_varslet"]
                and not post["kupp"]):
            populaere.append((fk, post, regel))

    ferdige = {fk: p for fk, p in biler.items() if _ferdig(p, naa)}
    for fk in ferdige:
        del biler[fk]
    spor["sist_kjort"] = naa_s
    grense = naa - timedelta(hours=KODE_HISTORIKK_TIMER)
    historikk = [[t, m] for t, m in historikk if _tid(t) >= grense]
    if maks_kode is not None:
        historikk.append([naa_s, int(maks_kode)])
    spor["kode_historikk"] = historikk
    populaere.sort(key=lambda x: -x[1]["maalinger"][-1]["fav"])
    return spor, populaere[:MAKS_VARSLER], ferdige


# ======================================================
# Varsel
# ======================================================

def melding(post: dict) -> str:
    def kr(v):
        try:
            return f"{int(round(float(v))):,}".replace(",", " ")
        except (TypeError, ValueError):
            return "?"
    siste = post["maalinger"][-1]
    navn = f"{post.get('Merke') or ''} {post.get('Modell') or ''}".strip()
    sted = f" – {post['sted']}" if post.get("sted") else ""
    linjer = [
        f"❤ {siste['fav']} favoritter etter {max(1, round(siste['alder_min']))} min",
        f"{navn} {post.get('Årstall') or '?'}, {kr(post.get('Kjørelengde'))} km{sted}",
    ]
    if post.get("forventet_pris") and post.get("rabatt_pct") is not None:
        linjer.append(f"{kr(post.get('Pris'))} kr ({-post['rabatt_pct']:+.0f}% mot "
                      f"{kr(post['forventet_pris'])})")
    else:
        linjer.append(f"{kr(post.get('Pris'))} kr")
    linjer.append(post.get("url") or "")
    return "\n".join(linjer)


# ======================================================
# S3 + kjøring
# ======================================================

def les_kode_historikk(s3) -> list:
    """Høyeste FINN-kode per kjøring (fra sporings-state), for kuppvakten."""
    return _les(s3, SPOR_KEY).get("kode_historikk", [])


def _les(s3, key: str) -> dict:
    try:
        obj = s3.get_object(Bucket=kupp.S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _skriv(s3, key: str, data: dict):
    s3.put_object(Bucket=kupp.S3_BUCKET, Key=key,
                  Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")


def _logg_ferdige(s3, ferdige: dict, naa: datetime):
    if not ferdige:
        return
    logg = _les(s3, LOGG_KEY)
    logg.update(ferdige)
    grense = naa - timedelta(days=kupp.LOGG_TTL_DAYS)
    logg = {fk: p for fk, p in logg.items()
            if _tid(p.get("forst_sett", naa.isoformat())) >= grense}
    _skriv(s3, LOGG_KEY, logg)


def kjor(s3, nye: list[dict], naa: str, *, dry_run: bool = False,
         kupp_koder=(), forhaandsmaalt: dict | None = None,
         maks_kode: int | None = None) -> int:
    """Oppdater sporing, send populær-varsler. Feil stopper aldri kuppvakten."""
    if not kupp.FAVORITTER_ON:
        return 0
    try:
        naa_dt = _tid(naa)
        spor = _les(s3, SPOR_KEY)
        session = kupp._make_session()
        try:
            spor, populaere, ferdige = oppdater(
                spor, nye, naa_dt, lambda fk: kupp.hent_favoritter(fk, session),
                kupp_koder=kupp_koder, forhaandsmaalt=forhaandsmaalt,
                maks_kode=maks_kode)
        finally:
            session.close()
        print(f"[favoritt_spor] {len(spor['biler'])} biler spores, "
              f"{len(ferdige)} ferdige, {len(populaere)} populære")
        if dry_run:
            for _, post, _ in populaere:
                print("[POPULÆR] " + melding(post).replace("\n", " | "))
            return len(populaere)
        for fk, post, (n, m) in populaere:
            ok = kupp._send_pushover([post], melding=melding(post),
                                     tittel=f"🔥 Populær bil ({n}+ favoritter på {m} min)")
            post["populaer_varslet"] = bool(ok)
        _skriv(s3, SPOR_KEY, spor)
        _logg_ferdige(s3, ferdige, naa_dt)
        return len(populaere)
    except Exception as e:
        print(f"[favoritt_spor] Advarsel: sporing feilet: {e}")
        return 0

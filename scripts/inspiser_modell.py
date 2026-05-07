"""
scripts/inspiser_modell.py
==========================
Inspeksjonsverktoy for BilRadar-prismodellen. To subkommandoer:

  segmenter  - dumper alle L1/L2-segmenter til CSV (med n_obs og mae_cv).
               Bra for a se hvilke modeller radaren har solid datagrunnlag for.

  probe      - tar en bestemt (merke, modell, drivstoff) og lager en priskurve
               over et rutenett av aar og km. Viser bade rapris og override-justert.

Eksempler:
    python -m scripts.inspiser_modell segmenter --output segmenter.csv
    python -m scripts.inspiser_modell probe --merke Tesla --modell "Model Y" \
        --drivstoff Elektrisk --hjuldrift Firehjulsdrift --karosseri SUV
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bilradar_modell_skjema import SEG_FEATURES_CAT, SEG_FEATURES_NUM  # noqa: E402
from bilradar_scorer import (  # noqa: E402
    last_modell_lokal_eller_s3,
    scorer_biler,
)

REPRESENTATIVE_KONFIGS = [
    ("pris_3aar_40k", 3, 40_000),
    ("pris_6aar_90k", 6, 90_000),
    ("pris_10aar_150k", 10, 150_000),
]

DEFAULT_MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "bil_prismodell.joblib",
)


def _representativ_pris(seg_model, alder: int, kjorelengde: int) -> float:
    """Predikerer pris for et standard-oppsett. Brukes til en "typisk pris"-kolonne
    i segmenter-dumpen. Bruker "Ukjent" for kategorier som ikke skiller mye —
    OneHotEncoder er konfigurert med handle_unknown='ignore' under trening."""
    row = {
        "alder": alder,
        "kjørelengde": kjorelengde,
        "km_per_aar": kjorelengde / max(alder, 1),
        "garanti_mnd": 0,
        "rekkevidde_km": 0,
        "hjuldrift": "Ukjent",
        "girkasse": "Automat",
        "Karosseri": "Ukjent",
    }
    X = pd.DataFrame([row]).reindex(columns=SEG_FEATURES_NUM + SEG_FEATURES_CAT)
    try:
        log_pred = seg_model.model.predict(X)
        return float(np.expm1(log_pred[0]))
    except Exception:
        return float("nan")


def _segmenter_til_df(modeller) -> pd.DataFrame:
    rader = []
    for kategori, seg_dict, niv in [
        ("market", modeller.market_l1, "L1"),
        ("market", modeller.market_l2, "L2"),
        ("fast", modeller.fast_l1, "L1"),
        ("fast", modeller.fast_l2, "L2"),
    ]:
        for key, sm in seg_dict.items():
            rad = {
                "kategori": kategori,
                "nivaa": niv,
                "segment": key,
                "n_obs": sm.n_obs,
                "label": sm.label,
                "mae_cv": sm.mae_cv,
            }
            for kol, alder, km in REPRESENTATIVE_KONFIGS:
                rad[kol] = round(_representativ_pris(sm, alder, km))
            rader.append(rad)
    df = pd.DataFrame(rader)
    if not df.empty:
        df = df.sort_values(["kategori", "nivaa", "n_obs"], ascending=[True, True, False])
    return df


def cmd_segmenter(args):
    modeller = last_modell_lokal_eller_s3(args.model)
    df = _segmenter_til_df(modeller)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"Skrev {len(df)} segmenter til {args.output}")
    print()
    print("Topp 20 markedssegmenter etter datagrunnlag (typiske priser):")
    print(f"  {'niv':>3}  {'n_obs':>5}  {'3aar/40k':>10}  {'6aar/90k':>10}  {'10aar/150k':>11}  segment")
    topp = df[df["kategori"] == "market"].head(20)
    for _, r in topp.iterrows():
        print(
            f"  {r['nivaa']:>3}  {r['n_obs']:>5}  "
            f"{int(r['pris_3aar_40k']):>10}  "
            f"{int(r['pris_6aar_90k']):>10}  "
            f"{int(r['pris_10aar_150k']):>11}  {r['segment']}"
        )


def cmd_probe(args):
    modeller = last_modell_lokal_eller_s3(args.model)

    aar_liste = list(range(args.aar_fra, args.aar_til + 1))
    km_liste = [int(x) for x in args.km.split(",")]

    rader = []
    for i, (aar, km) in enumerate([(a, k) for a in aar_liste for k in km_liste]):
        rader.append({
            "FinnKode": i + 1,
            "Merke": args.merke,
            "Modell": args.modell,
            "Drivstoff": args.drivstoff,
            "Hjuldrift": args.hjuldrift,
            "Girkasse": args.girkasse,
            "Karosseri": args.karosseri,
            "Forhandler": args.forhandler,
            "Fylke": args.fylke,
            "rekkevidde_km": args.rekkevidde,
            "Aarstall": aar,
            "Kjorelengde": km,
            "Pris": 1,
            "Info": "",
        })
    probe = pd.DataFrame(rader).rename(columns={
        "Aarstall": "årstall",
        "Kjorelengde": "kjørelengde",
    })

    scoret = scorer_biler(probe, modeller)

    cols = [
        "årstall", "kjørelengde",
        "forventet_pris_raa", "forventet_pris",
        "hurtigpris", "modell_nivaa", "peer_konfidens",
        "overstyrt", "overstyring_kommentar",
    ]
    cols = [c for c in cols if c in scoret.columns]
    ut = scoret[cols].copy()
    for c in ("forventet_pris_raa", "forventet_pris", "hurtigpris"):
        if c in ut.columns:
            ut[c] = ut[c].round().astype("Int64")

    if args.output:
        ut.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"Skrev {len(ut)} rader til {args.output}")

    print()
    print(f"Priskurve for {args.merke} {args.modell} ({args.drivstoff}, "
          f"{args.hjuldrift}, {args.karosseri}):")
    print(ut.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Inspiser BilRadar-prismodellen.")
    parser.add_argument("--model", default=DEFAULT_MODEL_PATH,
                        help=f"Sti til pkl/joblib (default: {DEFAULT_MODEL_PATH})")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_seg = sub.add_parser("segmenter", help="Dump alle L1/L2-segmenter til CSV.")
    p_seg.add_argument("--model", default=DEFAULT_MODEL_PATH)
    p_seg.add_argument("--output", default="segmenter.csv")
    p_seg.set_defaults(func=cmd_segmenter)

    p_pr = sub.add_parser("probe", help="Lag priskurve for en modell over aar/km.")
    p_pr.add_argument("--model", default=DEFAULT_MODEL_PATH)
    p_pr.add_argument("--merke", required=True)
    p_pr.add_argument("--modell", required=True)
    p_pr.add_argument("--drivstoff", required=True,
                      help="F.eks. Elektrisk, Bensin, Diesel, Hybrid")
    p_pr.add_argument("--hjuldrift", default="Ukjent")
    p_pr.add_argument("--girkasse", default="Automat")
    p_pr.add_argument("--karosseri", default="Ukjent")
    p_pr.add_argument("--forhandler", default="Forhandler")
    p_pr.add_argument("--fylke", default="Ukjent")
    p_pr.add_argument("--rekkevidde", type=float, default=0.0,
                      help="Brukes for EV. 0 hvis irrelevant.")
    p_pr.add_argument("--aar-fra", type=int, default=2018)
    p_pr.add_argument("--aar-til", type=int, default=2024)
    p_pr.add_argument("--km", default="10000,30000,60000,90000,120000",
                      help="Kommaseparert liste over km-verdier.")
    p_pr.add_argument("--output", default="",
                      help="Valgfri CSV-output (i tillegg til konsoll).")
    p_pr.set_defaults(func=cmd_probe)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

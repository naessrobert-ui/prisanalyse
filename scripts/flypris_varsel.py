#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Send e-postvarsel KUN når flyprismodellen finner noe av betydning.

Kjøres etter den daglige innsamlingen. Sender e-post bare hvis:
  * ett eller flere prisavvik er flagget (Norwegian-pris avviker unormalt), eller
  * prisspredningen (median−laveste) har endret seg markant, eller
  * Norwegian-prisindeksen har gjort et stort hopp siden forrige måling.

Ellers: ingenting (stille exit 0). Slik blir hver e-post et faktisk signal.

Miljøvariabler (gjenbruker appens SMTP-oppsett):
    SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASSWORD
    FLYPRIS_VARSEL_TIL  - mottaker(e), kommaseparert (fallback: MEDIA_DIGEST_TO)
    FLYPRIS_VARSEL_FRA  - avsender (default SMTP_USER / MEDIA_DIGEST_FROM)

Mangler SMTP/mottaker avsluttes jobben stille (exit 0).
"""

from __future__ import annotations

import html
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from scripts.flypriser_analyse import full_analyse

# Terskler for hva som er «av betydning»
INDEKS_HOPP = 3.0          # indekspoeng siden forrige måling
DISPERSJON_ENDRING = 5.0   # prosentpoeng endring i median−laveste-gap


def _mottakere() -> list[str]:
    raw = os.environ.get("FLYPRIS_VARSEL_TIL") or os.environ.get("MEDIA_DIGEST_TO") or ""
    return [a.strip() for a in raw.replace(";", ",").split(",") if a.strip()]


def vurder(analyse: dict) -> dict | None:
    """Returner en dict med grunner hvis noe er av betydning, ellers None."""
    if not analyse.get("ok"):
        return None
    s = analyse["sammendrag"]
    avvik = analyse.get("avvik", [])
    grunner: list[str] = []

    if avvik:
        opp = sum(1 for a in avvik if a["retning"] == "opp")
        ned = len(avvik) - opp
        grunner.append(f"{len(avvik)} prisavvik flagget ({opp} opp, {ned} ned)")

    idx_endr = s.get("norwegian_indeks_endring")
    if idx_endr is not None and abs(idx_endr) >= INDEKS_HOPP:
        grunner.append(f"Norwegian-indeks {idx_endr:+.1f} poeng")

    disp_endr = s.get("dispersjon_endring")
    if disp_endr is not None and abs(disp_endr) >= DISPERSJON_ENDRING:
        retning = "krympet (prispress)" if disp_endr < 0 else "økt"
        grunner.append(f"Prisspredning {retning} {abs(disp_endr):.1f} pp")

    if not grunner:
        return None
    return {"grunner": grunner, "sammendrag": s, "avvik": avvik}


def bygg_html(varsel: dict) -> str:
    s = varsel["sammendrag"]
    grunner = "".join(f"<li>{html.escape(g)}</li>" for g in varsel["grunner"])
    avvik_rader = "".join(
        "<li style='margin-bottom:6px'>"
        f"<strong>{html.escape(a['navn'])}</strong>: {a['pris']} kr "
        f"({a['endring_pst']:+}% mot normalt {a['basis']} kr)"
        "</li>"
        for a in varsel["avvik"][:15]
    )
    return (
        "<h2>✈️ Flypris-signal – Norwegian</h2>"
        f"<p>Modellen fanget opp følgende {s.get('sist_hentet','')}:</p>"
        f"<ul>{grunner}</ul>"
        + (f"<h3>Flaggede ruter</h3><ul>{avvik_rader}</ul>" if avvik_rader else "")
        + f"<p>Norwegian-indeks: <strong>{s.get('norwegian_indeks')}</strong> "
        f"(median {s.get('norwegian_median_indeks')}), "
        f"prisspredning {s.get('dispersjon_gap_pst')} %.</p>"
        "<p><a href='https://prisanalyse.no/flypriser/norwegian/'>Åpne investor-dashbordet</a></p>"
    )


def _send_mail(subject: str, html_body: str) -> bool:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    mottakere = _mottakere()
    if not smtp_host or not mottakere:
        print("Sending hoppet over: SMTP_HOST/FLYPRIS_VARSEL_TIL ikke satt.")
        return False

    smtp_user = os.environ.get("SMTP_USER", "")
    sender = os.environ.get("FLYPRIS_VARSEL_FRA") or os.environ.get("MEDIA_DIGEST_FROM") or smtp_user

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(mottakere)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    port = int(os.environ.get("SMTP_PORT", "587"))
    with smtplib.SMTP(smtp_host, port, timeout=30) as server:
        server.starttls()
        if smtp_user:
            server.login(smtp_user, os.environ.get("SMTP_PASSWORD", ""))
        server.sendmail(sender, mottakere, msg.as_string())
    print(f"E-post sendt til {', '.join(mottakere)} (fra {sender}).")
    return True


def send_epost(varsel: dict) -> bool:
    n = len(varsel["avvik"])
    subject = f"✈️ Flypris-signal Norwegian ({n} avvik)" if n else "✈️ Flypris-signal Norwegian"
    return _send_mail(subject, bygg_html(varsel))


def send_test() -> bool:
    """Send en bekreftelses-e-post for å verifisere SMTP-oppsettet."""
    html_body = (
        "<h2>✈️ Flypris-varsel – oppsettet virker</h2>"
        "<p>Dette er en testmelding. SMTP/Resend-oppsettet er korrekt, og du vil nå få "
        "e-post automatisk <strong>når modellen fanger et prissignal</strong> "
        "(flagget avvik, stort indeks-hopp eller markant endring i prisspredning).</p>"
        "<p><a href='https://prisanalyse.no/flypriser/norwegian/'>Åpne investor-dashbordet</a></p>"
    )
    return _send_mail("✈️ Flypris-varsel – testoppsett", html_body)


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if "--test" in args:
        print("Sender test-e-post …")
        send_test()
        return 0
    dry = "--dry-run" in args
    analyse = full_analyse()
    varsel = vurder(analyse)
    if varsel is None:
        print("Ingen signaler av betydning – ingen e-post sendt.")
        return 0
    print("Signal:", "; ".join(varsel["grunner"]))
    if dry:
        print("(--dry-run: sender ikke e-post)")
        return 0
    send_epost(varsel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

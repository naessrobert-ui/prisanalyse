"""Ukentlig e-postdigest fra media-arkivet for Robert Naess.

Sender en oppsummering av siste 7 døgn (antall treff, sentimentfordeling,
toppsaker) til MEDIA_DIGEST_TO. Kjøres mandager via Render cron.

Krever miljøvariabler:
    SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASSWORD
    MEDIA_DIGEST_TO   - mottakeradresse(r), kommaseparert
    MEDIA_DIGEST_FROM - avsender (default SMTP_USER)

Mangler konfigurasjonen avsluttes jobben stille med exit 0, slik at cron-en
kan ligge klar før e-post er satt opp.
"""

from __future__ import annotations

import html
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def _build_digest_html() -> str | None:
    from media_arkiv import archive_articles, load_archive
    from media_sources import cluster_articles, dedupe_articles

    df = load_archive(use_cache=False)
    articles = dedupe_articles(archive_articles(df, days=7))
    if not articles:
        return None

    sentiments = {"positiv": 0, "nøytral": 0, "negativ": 0, "ukjent": 0}
    for article in articles:
        sentiments[article.get("sentiment") or "ukjent"] = sentiments.get(article.get("sentiment") or "ukjent", 0) + 1

    clusters = sorted(
        cluster_articles(articles),
        key=lambda cluster: cluster["count"],
        reverse=True,
    )[:8]

    rows = []
    for cluster in clusters:
        primary = cluster["primary"]
        also = f" (også i {', '.join(cluster['also_in'])})" if cluster["also_in"] else ""
        published = primary.get("published_at")
        date_label = published.strftime("%d.%m") if published else ""
        rows.append(
            "<li style='margin-bottom:10px'>"
            f"<a href='{html.escape(primary.get('url') or '')}'>{html.escape(primary.get('title') or '')}</a>"
            f"<br><small>{html.escape(primary.get('domain') or '')} {date_label}{html.escape(also)}</small>"
            "</li>"
        )

    return (
        "<h2>Medieovervåking Robert Næss - siste 7 døgn</h2>"
        f"<p><strong>{len(articles)}</strong> treff. Sentiment: "
        f"{sentiments['positiv']} positive, {sentiments['nøytral']} nøytrale, "
        f"{sentiments['negativ']} negative, {sentiments['ukjent']} uvurderte.</p>"
        "<h3>Toppsaker</h3>"
        f"<ol>{''.join(rows)}</ol>"
        "<p><a href='https://prisanalyse.no/media/robert-naess/'>Åpne dashbordet</a></p>"
    )


def main() -> int:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    recipients = [addr.strip() for addr in os.environ.get("MEDIA_DIGEST_TO", "").split(",") if addr.strip()]
    if not smtp_host or not recipients:
        print("Digest hoppet over: SMTP_HOST/MEDIA_DIGEST_TO ikke satt.")
        return 0

    body = _build_digest_html()
    if body is None:
        print("Digest hoppet over: ingen treff i arkivet siste 7 døgn.")
        return 0

    smtp_user = os.environ.get("SMTP_USER", "")
    sender = os.environ.get("MEDIA_DIGEST_FROM", smtp_user) or smtp_user

    message = MIMEMultipart("alternative")
    message["Subject"] = "Medieovervåking Robert Næss - ukesoppsummering"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.attach(MIMEText(body, "html", "utf-8"))

    port = int(os.environ.get("SMTP_PORT", "587"))
    with smtplib.SMTP(smtp_host, port, timeout=30) as server:
        server.starttls()
        if smtp_user:
            server.login(smtp_user, os.environ.get("SMTP_PASSWORD", ""))
        server.sendmail(sender, recipients, message.as_string())

    print(f"Digest sendt til {', '.join(recipients)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

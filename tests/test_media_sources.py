from datetime import datetime, timedelta, timezone

import pandas as pd

from media_arkiv import archive_articles, enrichment_lookup, merge_articles, _empty_archive
from media_sources import article_id, cluster_articles, dedupe_articles, normalize_url, unwrap_redirect


def _article(title, url, *, domain="finansavisen.no", category="nyheter", days_ago=0, **extra):
    return {
        "title": title,
        "url": url,
        "source": domain,
        "domain": domain,
        "published_at": datetime.now(timezone.utc) - timedelta(days=days_ago),
        "snippet": extra.pop("snippet", ""),
        "origin": extra.pop("origin", "Google News"),
        "category": category,
        "matched_query": "",
        "engagement": extra.pop("engagement", 0),
        **extra,
    }


def test_normalize_url_stripper_tracking_og_www():
    assert normalize_url("https://www.e24.no/sak?utm_source=x&id=7#del") == "https://e24.no/sak?id=7"
    assert normalize_url("https://e24.no/sak/?id=7") == normalize_url("https://www.e24.no/sak?id=7&utm_medium=rss")


def test_article_id_stabil_paa_tvers_av_tracking():
    a = _article("Tittel", "https://e24.no/sak?utm_source=feed")
    b = _article("Tittel", "https://www.e24.no/sak")
    assert article_id(a) == article_id(b)


def test_dedupe_fjerner_duplikat_og_sorterer_nyeste_forst():
    gammel = _article("Gammel sak", "https://e24.no/gammel", days_ago=5)
    ny = _article("Ny sak", "https://e24.no/ny", days_ago=0)
    duplikat = _article("Ny sak igjen", "https://www.e24.no/ny?utm_source=x", days_ago=0)
    resultat = dedupe_articles([gammel, ny, duplikat])
    assert len(resultat) == 2
    assert resultat[0]["title"] == "Ny sak"


def test_unwrap_bing_redirect():
    wrapped = "http://www.bing.com/news/apiclick.aspx?ref=FexRss&aid=&tid=abc123&url=https%3a%2f%2ffinansavisen.no%2fsak&c=1&mkt=nb-no"
    assert unwrap_redirect(wrapped) == "https://finansavisen.no/sak"
    assert normalize_url(wrapped) == "https://finansavisen.no/sak"
    assert unwrap_redirect("https://e24.no/sak") == "https://e24.no/sak"


def test_dedupe_samme_artikkel_fra_google_og_bing():
    via_google = _article(
        "Robert Næss med Tesla-tips: – Bare å skynde seg - Nettavisen",
        "https://news.google.com/rss/articles/CBMiabc123",
        domain="nettavisen.no",
    )
    via_bing = _article(
        "Robert Næss med Tesla-tips: – Bare å skynde seg",
        "https://www.nettavisen.no/okonomi/tesla-tips/s/5-95-100",
        domain="nettavisen.no",
        origin="Bing News",
    )
    assert len(dedupe_articles([via_google, via_bing])) == 1


def test_cluster_grupperer_samme_sak_paa_tvers_av_medier():
    a = _article(
        "Stygg Softbank-smell trekker OpenAI-verdsettelse i tvil - Finansavisen",
        "https://finansavisen.no/sak1",
        snippet="utdrag",
    )
    b = _article(
        "Stygg Softbank-smell trekker OpenAI-verdsettelse i tvil - E24",
        "https://e24.no/sak2",
        domain="e24.no",
    )
    c = _article("Robert Næss anbefaler tre aksjer for sommeren", "https://dn.no/sak3", domain="dn.no")
    clusters = cluster_articles(dedupe_articles([a, b, c]))
    assert len(clusters) == 2
    softbank = next(cl for cl in clusters if "Softbank" in cl["primary"]["title"])
    assert softbank["count"] == 2
    assert softbank["primary"]["domain"] == "finansavisen.no"  # har snippet
    assert softbank["also_in"] == ["e24.no"]


def test_cluster_klynger_ikke_sosiale_poster():
    a = _article("Godt poeng fra Robert Næss om rentebanen i dag", "https://x.com/p/1", domain="x.com", category="sosialt")
    b = _article("Godt poeng fra Robert Næss om rentebanen i dag", "https://x.com/p/2", domain="x.com", category="sosialt")
    clusters = cluster_articles([a, b])
    assert len(clusters) == 2


def test_merge_articles_nye_rader_og_engagement_oppdatering():
    df = _empty_archive()
    a = _article("Sak", "https://e24.no/sak", category="sosialt", engagement=10)
    df, nye = merge_articles(df, [a])
    assert nye == 1

    a_oppdatert = dict(a, engagement=50)
    b = _article("Annen sak", "https://dn.no/annen", domain="dn.no")
    df, nye = merge_articles(df, [a_oppdatert, b])
    assert nye == 1
    assert len(df) == 2
    assert int(df.loc[df["url"] == "https://e24.no/sak", "engagement"].iloc[0]) == 50


def test_archive_roundtrip_og_enrichment_lookup():
    df = _empty_archive()
    df, _ = merge_articles(df, [_article("Sak", "https://e24.no/sak", days_ago=2)])
    df.loc[0, "sentiment"] = "positiv"
    df.loc[0, "tema"] = "aksjeråd"
    df.loc[0, "sitat"] = "-"

    artikler = archive_articles(df, days=7)
    assert len(artikler) == 1
    assert artikler[0]["sentiment"] == "positiv"
    assert artikler[0]["sitat"] == ""  # '-' betyr forsøkt uten funn

    lookup = enrichment_lookup(df)
    row_id = df.loc[0, "id"]
    assert lookup[row_id]["tema"] == "aksjeråd"

    assert archive_articles(df, days=1) == []

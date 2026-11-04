import datetime as dt
import json
import unittest
from unittest.mock import patch

from newsweb_monitor import importance, indirect_news, market_data
from newsweb_monitor.monitor import ai, clean_affected
from newsweb_monitor.publish_site import build_snapshot, merge_indirect, sanitize

UTC = dt.timezone.utc


def ts(hour, minute=0, day=4):
    return int(dt.datetime(2026, 11, day, hour, minute, tzinfo=UTC).timestamp())


def chart(bars, price):
    return {"bars": bars, "price": price, "currency": "NOK", "name": "Test ASA"}


class MarketDataTests(unittest.TestCase):
    def test_parse_chart_skips_missing_closes(self):
        payload = {"chart": {"result": [{"meta": {"regularMarketPrice": 12.0, "currency": "NOK"},
                   "timestamp": [1, 2, 3], "indicators": {"quote": [{"close": [10.0, None, 12.0],
                                                                      "volume": [5, 6, None]}]}}]}}
        data = market_data.parse_chart(payload)
        self.assertEqual(data["bars"], [(1, 10.0, 5), (3, 12.0, 0)])
        self.assertEqual(data["price"], 12.0)
        self.assertIsNone(market_data.parse_chart({"chart": {"result": None}}))

    def test_reaction_uses_last_trade_before_message_and_index(self):
        stock = chart([(ts(15, 0, 3), 100.0, 1000), (ts(8, 5), 110.0, 3000), (ts(9, 0), 112.0, 3000)], 112.0)
        index = chart([(ts(15, 0, 3), 1000.0, 0), (ts(9, 0), 1010.0, 0)], 1010.0)
        r = market_data.reaction("TEST", "2026-11-04T06:00:00Z", chart=stock, index_chart=index,
                                 now=dt.datetime(2026, 11, 4, 10, tzinfo=UTC))
        self.assertEqual(r["price_before"], 100.0)
        self.assertEqual(r["change_pct"], 12.0)
        self.assertEqual(r["index_pct"], 1.0)
        self.assertEqual(r["abnormal_pct"], 11.0)
        self.assertTrue(r["price_driver"])
        self.assertTrue(r["market_open"])
        self.assertEqual(r["rel_volume"], 6.0)

    def test_small_move_is_not_price_driver(self):
        stock = chart([(ts(7), 100.0, 1), (ts(9), 101.0, 1)], 101.0)
        index = chart([(ts(7), 100.0, 0), (ts(9), 100.5, 0)], 100.5)
        r = market_data.reaction("TEST", "2026-11-04T08:00:00Z", chart=stock, index_chart=index)
        self.assertFalse(r["price_driver"])

    def test_no_trade_before_message(self):
        stock = chart([(ts(9), 101.0, 1)], 101.0)
        r = market_data.reaction("TEST", "2026-11-04T08:00:00Z", chart=stock, index_chart=stock)
        self.assertIn("error", r)

    def test_symbols(self):
        self.assertEqual(market_data.yahoo_symbol("mowi"), "MOWI.OL")
        self.assertEqual(market_data.yahoo_symbol("OSEBX"), "OSEBX.OL")
        self.assertIsNone(market_data.yahoo_symbol("TORM"))
        self.assertIsNone(market_data.yahoo_symbol("../x"))

    def test_market_hours(self):
        oslo = market_data.OSLO
        self.assertTrue(market_data.market_open(dt.datetime(2026, 11, 4, 10, tzinfo=oslo)))
        self.assertFalse(market_data.market_open(dt.datetime(2026, 11, 4, 8, tzinfo=oslo)))
        self.assertFalse(market_data.market_open(dt.datetime(2026, 11, 7, 12, tzinfo=oslo)))


class ImportanceTests(unittest.TestCase):
    def test_large_cap_boost_keeps_ai_score(self):
        entry = importance.apply({"ticker": "EQNR", "score": 70})
        self.assertEqual((entry["score"], entry["score_ai"], entry["importance"]), (80, 70, "stor"))

    def test_trivial_news_not_boosted(self):
        entry = importance.apply({"ticker": "EQNR", "score": 20})
        self.assertEqual(entry["score"], 20)
        self.assertNotIn("score_ai", entry)

    def test_unknown_company_unchanged(self):
        self.assertEqual(importance.apply({"ticker": "XYZ", "score": 70}), {"ticker": "XYZ", "score": 70})

    def test_sector_peers_exclude_self(self):
        peers = importance.sector_peers("MOWI")
        self.assertIn("SALM", peers["Laks"])
        self.assertNotIn("MOWI", peers["Laks"])


class AffectedTests(unittest.TestCase):
    def test_clean_affected(self):
        rows = [{"ticker": "salm", "direction": "positiv", "reason": "Samme prisbilde"},
                {"ticker": "MOWI", "direction": "positiv", "reason": "egen"},
                {"ticker": "SALM", "direction": "negativ", "reason": "dublett"},
                {"ticker": "bad ticker!", "direction": "x", "reason": ""},
                {"ticker": "LSG", "direction": "sterk", "reason": "x" * 300}]
        out = clean_affected(rows, "MOWI")
        self.assertEqual([r["ticker"] for r in out], ["SALM", "LSG"])
        self.assertEqual(out[1]["direction"], "uklar")
        self.assertEqual(len(out[1]["reason"]), 160)

    def test_ai_returns_affected_and_sends_sector_hint(self):
        sample = {"issuer": "Mowi ASA", "ticker": "MOWI", "title": "Q3", "body": "Rekordvolum",
                  "published_at": "2026-11-04T05:30:00Z"}
        response = {"output": [{"content": [{"type": "output_text", "text": json.dumps({
            "headline": "Mowi slakter rekordvolum", "lead": "Volumet økte 12 prosent.",
            "score": 70, "direction": "positiv", "reason": "Høyere volum.",
            "affected": [{"ticker": "SALM", "direction": "positiv", "reason": "Samme laksepris"}]})}]}]}
        with patch("newsweb_monitor.monitor.http_json", return_value=response) as call:
            result = ai(sample, "test-key")
        self.assertEqual(result["affected"][0]["ticker"], "SALM")
        sent = json.loads(call.call_args[0][1]["input"])
        self.assertIn("SALM", sent["sector_peers"]["Laks"])


RSS = """<?xml version="1.0"?><rss><channel>
<item><title>Olje- og gassprisen faller brått</title><link>https://e24.no/a/1</link>
<description>&lt;p&gt;Prisfallet drar Oslo Børs ned.&lt;/p&gt;</description>
<pubDate>Wed, 04 Nov 2026 09:00:00 GMT</pubDate></item>
<item><title>Uten lenke</title><link>http://ikke-https</link></item>
</channel></rss>""".encode("utf-8")


class IndirectTests(unittest.TestCase):
    def test_parse_rss(self):
        items = indirect_news.parse_rss(RSS, "E24")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["teaser"], "Prisfallet drar Oslo Børs ned.")
        self.assertEqual(items[0]["published_at"], "2026-11-04T09:00:00+00:00")

    def test_clean_requires_affected_and_score(self):
        cand = indirect_news.parse_rss(RSS, "E24")
        result = {"items": [
            {"index": 0, "score": 70, "comment": "Lavere oljepris",
             "affected": [{"ticker": "eqnr", "direction": "negativ", "reason": "Lavere inntekt"}]},
            {"index": 0, "score": 30, "comment": "for lav", "affected": [{"ticker": "EQNR"}]},
            {"index": 5, "score": 90, "comment": "ugyldig indeks", "affected": [{"ticker": "EQNR"}]}]}
        rows = indirect_news.clean(result, cand, "2026-11-04T10:00:00+00:00")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["affected"][0]["ticker"], "EQNR")

    def test_run_marks_seen_and_skips_without_key(self):
        cand = indirect_news.parse_rss(RSS, "E24")
        state = {}
        now = dt.datetime(2026, 11, 4, 10, tzinfo=UTC)
        with patch.object(indirect_news, "fetch_feeds", return_value=cand):
            self.assertEqual(indirect_news.run(state, "", now=now), [])
            self.assertEqual(state["indirect_seen"], {})
            answer = {"output": [{"content": [{"type": "output_text", "text": json.dumps({"items": [
                {"index": 0, "score": 70, "comment": "Lavere oljepris",
                 "affected": [{"ticker": "EQNR", "direction": "negativ", "reason": "x"}]}]})}]}]}
            with patch("newsweb_monitor.monitor.http_json", return_value=answer):
                rows = indirect_news.run(state, "k", now=now)
            self.assertEqual(len(rows), 1)
            self.assertIn(cand[0]["id"], state["indirect_seen"])
            with patch("newsweb_monitor.monitor.http_json") as again:
                self.assertEqual(indirect_news.run(state, "k", now=now), [])
                again.assert_not_called()


class PublishTests(unittest.TestCase):
    def base(self):
        return {"id": "1", "market": "XOSL", "ticker": "MOWI", "issuer": "Mowi ASA", "title": "Q3",
                "published_at": "2026-11-04T05:30:00Z", "score": 80, "score_ai": 70,
                "importance": "stor", "url": "https://newsweb.oslobors.no/message/1",
                "affected": [{"ticker": "SALM", "direction": "positiv", "reason": "Samme laksepris"}]}

    def test_sanitize_keeps_affected_and_importance(self):
        cleaned = sanitize(self.base())
        self.assertEqual(cleaned["affected"][0]["ticker"], "SALM")
        self.assertEqual((cleaned["importance"], cleaned["score_ai"]), ("stor", 70))

    def test_indirect_merged_and_aged_out(self):
        now = dt.datetime(2026, 11, 4, 12, tzinfo=UTC)
        new = {"id": "a" * 16, "source": "E24", "title": "Oljeprisen faller", "url": "https://e24.no/a",
               "published_at": "2026-11-04T09:00:00+00:00", "score": 70, "comment": "x",
               "affected": [{"ticker": "EQNR", "direction": "negativ", "reason": "y"}]}
        old = {**new, "id": "b" * 16, "published_at": "2026-10-28T09:00:00+00:00"}
        bad = {**new, "id": "c" * 16, "url": "javascript:alert(1)"}
        rows = merge_indirect([old], [new, bad], now=now)
        self.assertEqual([r["id"] for r in rows], ["a" * 16])

    def test_snapshot_contains_indirect(self):
        new = {"id": "a" * 16, "source": "E24", "title": "Oljeprisen faller", "url": "https://e24.no/a",
               "published_at": dt.datetime.now(UTC).isoformat(), "score": 70, "comment": "x",
               "affected": [{"ticker": "EQNR", "direction": "negativ", "reason": "y"}]}
        snap, changed = build_snapshot({"source": "NEWSWEB", "items": [self.base()], "indirect": [new]})
        self.assertTrue(changed)
        self.assertEqual(len(snap["indirect"]), 1)


if __name__ == "__main__":
    unittest.main()


class NonAsaTests(unittest.TestCase):
    def test_non_asa_penalized(self):
        e = importance.apply({"ticker": "TORM", "issuer": "TORM plc", "score": 85})
        self.assertEqual((e["score"], e["score_ai"], e["company_form"]), (70, 85, "ikke ASA"))

    def test_important_foreign_company_not_penalized(self):
        e = importance.apply({"ticker": "FRO", "issuer": "Frontline plc", "score": 70})
        self.assertEqual(e["score"], 80)
        self.assertNotIn("company_form", e)

    def test_savings_bank_and_asa_unchanged(self):
        for issuer in ("SpareBank 1 Helgeland", "Sparebanken Vest", "BEWI ASA"):
            e = importance.apply({"ticker": "ABC", "issuer": issuer, "score": 60})
            self.assertEqual(e["score"], 60, issuer)


class DriverTests(unittest.TestCase):
    NOW = dt.datetime(2026, 11, 4, 11, 0, tzinfo=market_data.OSLO)

    def charts(self):
        base = [(ts(15, 0, 3), 100.0, 10), (ts(9, 0), None, 0)]
        def c(after):
            return chart([(ts(15, 0, 3), 100.0, 10), (ts(9, 0), after, 50)], after)
        return {"BEWI": c(112.0), "SALM": c(103.0), "LSG": c(100.2), "MOWI": c(101.0),
                "ZZZ": c(100.5), "OLD": c(130.0), market_data.INDEX: c(100.0)}

    def test_session_start(self):
        self.assertEqual(market_data.session_start(self.NOW).isoformat(), "2026-11-03T16:20:00+01:00")
        sat = dt.datetime(2026, 11, 7, 12, tzinfo=market_data.OSLO)
        self.assertEqual(market_data.session_start(sat).date().isoformat(), "2026-11-05")

    def test_drivers_with_peers_and_signals(self):
        charts = self.charts()
        items = [
            {"ticker": "BEWI", "issuer": "BEWI ASA", "headline": "BEWI øker EBITDA", "score": 85,
             "published_at": "2026-11-04T06:00:00Z",
             "affected": [{"ticker": "SALM", "direction": "positiv", "reason": "test"}]},
            {"ticker": "ZZZ", "issuer": "Z ASA", "title": "liten", "score": 90, "published_at": "2026-11-04T06:00:00Z"},
            {"ticker": "OLD", "issuer": "Old ASA", "title": "gammel", "score": 90, "published_at": "2026-11-02T06:00:00Z"},
            {"ticker": "SKIP", "skipped": True, "score": 0, "published_at": "2026-11-04T06:00:00Z"},
        ]
        out = market_data.drivers(items, now=self.NOW, fetch=charts.get,
                                  peers_for=lambda t: {"Test": ["BEWI", "LSG", "MOWI"]})
        self.assertEqual([d["ticker"] for d in out["drivers"]], ["BEWI"])
        peers = {p["ticker"]: p for p in out["drivers"][0]["peers"]}
        self.assertEqual(list(peers), ["SALM", "LSG", "MOWI"])
        self.assertEqual(peers["SALM"]["signal"], "følger")
        self.assertEqual(peers["SALM"]["basis"], "AI")
        self.assertEqual(peers["LSG"]["signal"], "etterslep")
        self.assertEqual(peers["MOWI"]["basis"], "sektor")

    def test_high_score_before_open_waits(self):
        before = dt.datetime(2026, 11, 4, 8, 0, tzinfo=market_data.OSLO)
        stock = chart([(ts(15, 0, 3), 100.0, 10)], 100.0)
        items = [{"ticker": "BEWI", "issuer": "BEWI ASA", "title": "x", "score": 85,
                  "published_at": "2026-11-04T06:00:00Z"},
                 {"ticker": "LOW", "issuer": "L ASA", "title": "y", "score": 50,
                  "published_at": "2026-11-04T06:00:00Z"}]
        out = market_data.drivers(items, now=before, fetch=lambda t: stock, peers_for=lambda t: {})
        self.assertEqual([(d["ticker"], d["status"]) for d in out["drivers"]], [("BEWI", "venter på handel")])


class TickerValidationTests(unittest.TestCase):
    ISSUERS = {"AKRBP": "Aker BP ASA", "KOG": "Kongsberg Gruppen ASA", "KOA": "Kongsberg Automotive ASA",
               "EQNR": "Equinor ASA", "SALM": "SalMar ASA", "DNB": "DNB Bank ASA"}

    def test_resolve(self):
        from newsweb_monitor.newsweb_source import resolve_ticker
        self.assertEqual(resolve_ticker("eqnr", self.ISSUERS), "EQNR")
        self.assertEqual(resolve_ticker("AKERBP", self.ISSUERS), "AKRBP")
        self.assertIsNone(resolve_ticker("KONGSBERG", self.ISSUERS))  # to ASA-treff: tvetydig
        self.assertIsNone(resolve_ticker("MAERSK", self.ISSUERS))
        self.assertEqual(resolve_ticker("MAERSK", {}), "MAERSK")  # uten liste: ingen validering

    def test_clean_affected_drops_unknown(self):
        rows = [{"ticker": "AKERBP", "direction": "negativ", "reason": "olje"},
                {"ticker": "MAERSK", "direction": "positiv", "reason": "ikke notert"},
                {"ticker": "EQNR", "direction": "negativ", "reason": "olje"}]
        out = clean_affected(rows, "VAR", self.ISSUERS)
        self.assertEqual([r["ticker"] for r in out], ["AKRBP", "EQNR"])

    def test_indirect_clean_validates(self):
        cand = indirect_news.parse_rss(RSS, "E24")
        result = {"items": [{"index": 0, "score": 70, "comment": "x", "affected": [
            {"ticker": "HUF1", "direction": "negativ", "reason": "?"},
            {"ticker": "AKERBP", "direction": "negativ", "reason": "olje"}]}]}
        rows = indirect_news.clean(result, cand, "t", self.ISSUERS)
        self.assertEqual([a["ticker"] for a in rows[0]["affected"]], ["AKRBP"])

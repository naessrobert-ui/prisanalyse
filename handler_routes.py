# handler_routes.py
"""
Flask Blueprint for Handler Oslo Børs.
Erstatter den gamle Streamlit-appen med native Flask-routes.

Registrer i app.py:
    from handler_routes import handler_bp
    app.register_blueprint(handler_bp)
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
from flask import (
    Blueprint,
    make_response,
    render_template,
    request,
    jsonify,
    Response,
)

import handler_data as hd
import handler_data_beste as hdb

_LOG = logging.getLogger(__name__)

_BEST_VIKTIGE_SELECTIONS: dict[str, dict] = {}
_BEST_VIKTIGE_SELECTION_TTL_SEC = 30 * 60

handler_bp = Blueprint(
    "handler",
    __name__,
    template_folder="templates",
    url_prefix="/handler-oslo-bors",
)


_BV_INVESTOR_CACHE: dict[str, dict] = {}
_BV_INVESTOR_CACHE_TTL_SECONDS = 15 * 60

_BESTE_TX_CACHE: dict[str, dict] = {}
_BESTE_TX_CACHE_TTL_SECONDS = 30 * 60


# =========================================================
# Helpers
# =========================================================
def _parse_date(s: str | None, default: dt.date) -> dt.date:
    if not s:
        return default
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        return default


def _csv_response(df: pd.DataFrame, filename: str) -> Response:
    buf = df.to_csv(index=False, sep=";", decimal=",")
    return Response(
        buf,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _check_db() -> str | None:
    """Return error message if DB not available, else None."""
    if not hd.db_available():
        diag = hd.db_diagnostics()
        _LOG.warning("Handler DB utilgjengelig: %s", diag)
        s3_uri = diag.get("s3_uri_parsed") or diag.get("s3_uri_raw") or "(ikke satt)"
        parse_hint = ""
        if diag.get("s3_parse_error"):
            parse_hint = f" Ugyldig S3-format: {diag['s3_parse_error']}."
        return (
            "Database ikke tilgjengelig. Sett HANDLER_LOCAL_DB_PATH til full filsti på Render-serveren "
            f"(nå satt til: {diag['path']}). "
            "Sti på din lokale PC (f.eks. C:\\...) kan ikke leses fra Render. "
            f"S3-plassering styres av HANDLER_DB_S3_URI (nå: {s3_uri})."
            f"{parse_hint}"
        )


def _cache_key_for_beste_viktige(list_name: str, date_from: dt.date, date_to: dt.date) -> str:
    base = f"{list_name}|{date_from.isoformat()}|{date_to.isoformat()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _default_list_filename() -> str:
    files = hd.list_csv_files()
    if not files:
        return "Beste.csv"
    if "Beste.csv" in files:
        return "Beste.csv"
    return files[0]


def _normalize_list_filename(raw: str | None) -> str:
    v = (raw or "").strip()
    if not v:
        return _default_list_filename()
    if not v.lower().endswith(".csv"):
        v = f"{v}.csv"
    return Path(v).name


def _is_beste_file(list_filename: str) -> bool:
    return Path(list_filename).stem.strip().lower() == "beste"


def _investor_cache_key_for_list(list_filename: str) -> str:
    list_path = hd.resolve_list_csv_path(list_filename)
    if not list_path:
        return list_filename
    try:
        mtime = int(os.path.getmtime(list_path))
    except OSError:
        mtime = 0
    return f"{list_filename}|{mtime}"


def _read_list_file_content(list_filename: str) -> str:
    list_path = hd.resolve_list_csv_path(list_filename)
    if not list_path:
        raise FileNotFoundError(f"Fant ikke listefil: {list_filename}")
    with open(list_path, "r", encoding="latin-1", newline="") as f:
        return f.read()


def _write_list_file_content(list_filename: str, content: str) -> str:
    safe_name = Path(list_filename).name
    target_dir = Path(hd.HANDLER_LIST_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / safe_name
    with open(target_path, "w", encoding="latin-1", newline="") as f:
        f.write(content)
    _BV_INVESTOR_CACHE.clear()
    return str(target_path)


def _get_cached_investor_ids(cache_key: str) -> list[str] | None:
    row = _BV_INVESTOR_CACHE.get(cache_key)
    if not row:
        return None
    if row.get("expires_at", 0) < time.time():
        _BV_INVESTOR_CACHE.pop(cache_key, None)
        return None
    return row.get("investor_ids")


def _set_cached_investor_ids(cache_key: str, investor_ids: list[str]) -> None:
    _BV_INVESTOR_CACHE[cache_key] = {
        "investor_ids": investor_ids,
        "expires_at": time.time() + _BV_INVESTOR_CACHE_TTL_SECONDS,
    }


def _cache_key_for_beste_transactions(
    investor_id: str,
    date_from: dt.date,
    date_to: dt.date,
    isin_filter: list[str] | None,
) -> str:
    isin_key = ",".join(sorted(isin_filter or []))
    base = f"{investor_id}|{date_from.isoformat()}|{date_to.isoformat()}|{isin_key}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]


def _get_cached_beste_transactions(cache_key: str) -> dict | None:
    row = _BESTE_TX_CACHE.get(cache_key)
    if not row:
        return None
    if row.get("expires_at", 0) < time.time():
        _BESTE_TX_CACHE.pop(cache_key, None)
        return None
    return row.get("payload")


def _set_cached_beste_transactions(cache_key: str, payload: dict) -> None:
    _BESTE_TX_CACHE[cache_key] = {
        "payload": payload,
        "expires_at": time.time() + _BESTE_TX_CACHE_TTL_SECONDS,
    }


def _load_investor_ids_for_beste_viktige(
    conn,
    list_filename: str,
    date_from: dt.date,
    date_to: dt.date,
) -> list[str]:
    cache_key = _investor_cache_key_for_list(list_filename)
    cached = _get_cached_investor_ids(cache_key)
    if cached is not None:
        return cached

    list_path = hd.resolve_list_csv_path(list_filename)
    if not list_path:
        return []

    df_list = hd.read_csv_guess(list_path)
    owner_mode = "Beste" if _is_beste_file(list_filename) else "Viktige"
    patterns = hd.extract_owner_patterns(owner_mode, df_list)
    investor_ids = hd.resolve_investor_ids(conn, patterns)
    if investor_ids:
        _set_cached_investor_ids(cache_key, investor_ids)
    return investor_ids


# =========================================================
# Forside / oversikt
# =========================================================
@handler_bp.route("/")
def handler_index():
    db_err = _check_db()
    diag = hd.db_diagnostics()
    return render_template(
        "handler/index.html",
        db_error=db_err,
        db_path=diag["path"],
        db_s3_uri=diag.get("s3_uri_parsed") or hd.HANDLER_DB_S3_URI,
        db_s3_parse_error=diag.get("s3_parse_error"),
        db_parent_exists=diag["parent_exists"],
        db_s3_upload_enabled=bool(hd.HANDLER_DB_S3_URI),
    )


@handler_bp.route("/api/upload-db-to-s3", methods=["POST"])
def api_upload_db_to_s3():
    if not hd.HANDLER_DB_S3_URI:
        return jsonify({"error": "HANDLER_DB_S3_URI er ikke satt på serveren."}), 400

    db_file = request.files.get("db_file")
    if not db_file or not db_file.filename:
        return jsonify({"error": "Velg en fil først."}), 400

    filename = os.path.basename(db_file.filename)
    if not filename.lower().endswith(".db"):
        return jsonify({"error": "Filen må være en SQLite .db-fil."}), 400

    payload = db_file.read()
    if not payload:
        return jsonify({"error": "Filen er tom."}), 400

    try:
        s3_uri = hd.upload_db_bytes_to_s3(payload, filename=filename)
    except Exception as exc:
        _LOG.exception("Feil ved opplasting av handler-db til S3")
        return jsonify({"error": f"Klarte ikke laste opp til S3: {exc}"}), 500

    try:
        local_db = io.BytesIO(payload)
        with open(hd.HANDLER_DB_PATH, "wb") as f:
            f.write(local_db.getbuffer())
    except Exception:
        _LOG.warning("S3-opplasting ok, men klarte ikke oppdatere lokal DB-cache", exc_info=True)

    return jsonify({
        "ok": True,
        "message": "Ny DB er lastet opp til S3.",
        "s3_uri": s3_uri,
    })


# =========================================================
# API: Investor-søk (brukes av flere sider via AJAX)
# =========================================================
@handler_bp.route("/api/search-investors")
def api_search_investors():
    q = request.args.get("q", "")
    err = _check_db()
    if err:
        return jsonify({"error": err}), 503
    conn = hd.db_connect()
    results = hd.search_investors(conn, q)
    conn.close()
    return jsonify(results)


@handler_bp.route("/api/search-securities")
def api_search_securities():
    q = request.args.get("q", "")
    err = _check_db()
    if err:
        return jsonify({"error": err}), 503
    conn = hd.db_connect()
    results = hd.search_securities(conn, q)
    conn.close()
    return jsonify(results)


# =========================================================
# 1) Handler per eier
# =========================================================
@handler_bp.route("/per-eier")
def per_eier_page():
    return render_template("handler/per_eier.html")


@handler_bp.route("/api/per-eier")
def api_per_eier():
    investor_id = request.args.get("investor_id", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())

    if not investor_id:
        return jsonify({"error": "Velg en investor"}), 400

    err = _check_db()
    if err:
        return jsonify({"error": err}), 503

    conn = hd.db_connect()
    df = hd.fetch_handler_per_eier(conn, investor_id, date_from, date_to)
    conn.close()

    if df.empty:
        return jsonify({"rows": [], "message": "Ingen handler i perioden"})

    # Round for display
    df["netto_mnok"] = df["netto_mnok"].round(2)
    df["brutto_mnok"] = df["brutto_mnok"].round(2)

    return jsonify({
        "rows": df[["ticker","isin","navn","antall_obs","netto_antall","netto_mnok","brutto_mnok"]].to_dict("records"),
        "count": len(df),
    })


@handler_bp.route("/api/per-eier/detaljer")
def api_per_eier_detaljer():
    investor_id = request.args.get("investor_id", "").strip()
    isin = request.args.get("isin", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())

    if not investor_id or not isin:
        return jsonify({"error": "Mangler investor_id eller isin"}), 400

    conn = hd.db_connect()
    df = hd.fetch_eier_transactions(conn, investor_id, isin, date_from, date_to)
    conn.close()

    if df.empty:
        return jsonify({"rows": [], "sum_mnok": 0})

    df["belop_mnok"] = df["belop_mnok"].round(2)
    return jsonify({
        "rows": df[["dato","antall","kurs","belop_mnok"]].to_dict("records"),
        "sum_mnok": round(df["belop"].sum() / 1_000_000, 2),
    })


@handler_bp.route("/api/per-eier/csv")
def api_per_eier_csv():
    investor_id = request.args.get("investor_id", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    conn = hd.db_connect()
    df = hd.fetch_handler_per_eier(conn, investor_id, date_from, date_to)
    conn.close()
    return _csv_response(df, f"handler_per_eier_{investor_id}.csv")


# =========================================================
# 2) Handler per aksje
# =========================================================
@handler_bp.route("/per-aksje")
def per_aksje_page():
    return render_template("handler/per_aksje.html")


@handler_bp.route("/api/per-aksje")
def api_per_aksje():
    isin = request.args.get("isin", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    top_n = int(request.args.get("top_n", 30))

    if not isin:
        return jsonify({"error": "Velg en aksje"}), 400

    conn = hd.db_connect()
    df = hd.fetch_handler_per_aksje(conn, isin, date_from, date_to)
    conn.close()

    if df.empty:
        return jsonify({"buy": [], "sell": [], "message": "Ingen data i perioden"})

    for c in ["kjop_mnok","salg_mnok","netto_mnok"]:
        df[c] = df[c].round(1)

    cols = ["eier","investor_type","antall_obs","kjop_antall","kjop_mnok","salg_antall","salg_mnok","netto_mnok"]

    buy = df[df["netto_mnok"] > 0].sort_values("netto_mnok", ascending=False).head(top_n)
    sell = df[df["netto_mnok"] < 0].sort_values("netto_mnok", ascending=True).head(top_n)

    return jsonify({
        "buy": buy[cols].to_dict("records"),
        "sell": sell[cols].to_dict("records"),
    })


@handler_bp.route("/api/per-aksje/csv")
def api_per_aksje_csv():
    isin = request.args.get("isin", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    conn = hd.db_connect()
    df = hd.fetch_handler_per_aksje(conn, isin, date_from, date_to)
    conn.close()
    return _csv_response(df, f"handler_per_aksje_{isin}.csv")


# =========================================================
# 3) Eier oversikt
# =========================================================
@handler_bp.route("/eier-oversikt")
def eier_oversikt_page():
    return render_template("handler/eier_oversikt.html")


@handler_bp.route("/api/eier-oversikt/per-eier")
def api_eier_oversikt_per_eier():
    investor_id = request.args.get("investor_id", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())

    if not investor_id:
        return jsonify({"error": "Velg investor"}), 400

    conn = hd.db_connect()
    df = hd.fetch_eier_oversikt_per_security(conn, investor_id, date_from, date_to)
    ts = hd.fetch_eier_oversikt_timeseries(conn, investor_id, date_from, date_to)
    conn.close()

    if df.empty:
        return jsonify({"rows": [], "timeseries": [], "message": "Ingen data"})

    df["netto_mnok"] = df["netto_mnok"].round(2)
    df["brutto_mnok"] = df["brutto_mnok"].round(2)

    ts_data = []
    if not ts.empty:
        ts_data = ts[["dato","netto_mnok"]].round(2).to_dict("records")

    return jsonify({
        "rows": df[["ticker","isin","navn","antall_obs","netto_antall","netto_mnok","brutto_mnok"]].to_dict("records"),
        "timeseries": ts_data,
    })


@handler_bp.route("/api/eier-oversikt/per-aksje")
def api_eier_oversikt_per_aksje():
    isin = request.args.get("isin", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())

    if not isin:
        return jsonify({"error": "Velg aksje"}), 400

    conn = hd.db_connect()
    df = hd.fetch_aksje_oversikt_per_investor(conn, isin, date_from, date_to)
    conn.close()

    if df.empty:
        return jsonify({"rows": [], "message": "Ingen data"})

    df["netto_mnok"] = df["netto_mnok"].round(2)
    return jsonify({
        "rows": df[["navn","investor_id","antall_obs","netto_antall","netto_mnok"]].to_dict("records"),
    })


# =========================================================
# 4) Handler de beste / viktige
# =========================================================
@handler_bp.route("/beste-viktige")
def beste_viktige_page():
    csv_files = hd.list_csv_files()
    return render_template("handler/beste_viktige.html", csv_files=csv_files)


@handler_bp.route("/api/beste-viktige")
def api_beste_viktige():
    t0 = time.perf_counter()
    list_name = _normalize_list_filename(request.args.get("list_name"))
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    try:
        top_n = int(request.args.get("top_n", 10))
    except (TypeError, ValueError):
        top_n = 10
    top_n = max(10, min(top_n, 200))
    query_key = _cache_key_for_beste_viktige(list_name, date_from, date_to)

    list_path = hd.resolve_list_csv_path(list_name)

    if not list_path:
        return jsonify({
            "error": (
                f"Fant ikke listefil: {list_name}. "
                f"Søkt lokalt i {hd.HANDLER_LIST_DIR} og evt. S3-prefix {hd.HANDLER_LIST_S3_PREFIX or '(ikke satt)'}"
            )
        }), 404

    t_conn = time.perf_counter()
    conn = hd.db_connect()
    t_ids_start = time.perf_counter()
    investor_ids = _load_investor_ids_for_beste_viktige(conn, list_name, date_from, date_to)
    t_ids = time.perf_counter()

    if not investor_ids:
        conn.close()
        return jsonify({"error": "Fant ingen investorer som matcher listen"}), 404

    summary = hd.fetch_best_viktige_summary(conn, investor_ids, date_from, date_to)
    t_summary = time.perf_counter()
    conn.close()

    if summary.empty:
        _LOG.info("beste-viktige timing list=%s conn=%.3fs ids=%.3fs summary=%.3fs total=%.3fs (empty)", list_name, t_ids_start-t_conn, t_ids-t_ids_start, t_summary-t_ids, t_summary-t0)
        return jsonify({"buy": [], "sell": [], "message": "Ingen handler i perioden"})

    for c in ["kjop_mnok","salg_mnok","netto_mnok","brutto_mnok"]:
        summary[c] = summary[c].round(1)

    cols = ["ticker","isin","navn","antall_obs","kjop_mnok","salg_mnok","netto_mnok"]

    buy = summary[summary["netto_belop"]>0].sort_values("netto_belop", ascending=False).head(top_n)
    sell = summary[summary["salg_belop"]>0].sort_values("salg_belop", ascending=False).head(top_n)

    detail_options_df = summary[["ticker", "isin", "navn"]].copy()
    detail_options_df = detail_options_df.drop_duplicates(subset=["isin"])
    detail_options_df = detail_options_df.sort_values(["ticker", "navn", "isin"], ascending=[True, True, True])
    detail_options = detail_options_df.assign(
        label=lambda d: d["ticker"].fillna("") + " | " + d["navn"].fillna("") + " | " + d["isin"].fillna("")
    )[["isin", "label"]].to_dict("records")

    t_end = time.perf_counter()
    _LOG.info("beste-viktige timing list=%s conn=%.3fs ids=%.3fs summary=%.3fs format=%.3fs total=%.3fs investors=%d rows=%d", list_name, t_ids_start-t_conn, t_ids-t_ids_start, t_summary-t_ids, t_end-t_summary, t_end-t0, len(investor_ids), len(summary))

    resp = make_response(jsonify({
        "buy": buy[cols].to_dict("records"),
        "sell": sell[cols].to_dict("records"),
        "investor_count": len(investor_ids),
        "detail_options": detail_options,
        "query_key": query_key,
        "list_name": list_name,
    }))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@handler_bp.route("/api/beste-viktige/details")
def api_beste_viktige_details():
    t0 = time.perf_counter()
    list_name = _normalize_list_filename(request.args.get("list_name"))
    date_from = _parse_date(request.args.get("date_from"), dt.date.today() - dt.timedelta(days=30))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    isin = request.args.get("isin", "").strip()
    query_key = request.args.get("query_key", "").strip()

    if not isin:
        return jsonify({"error": "Velg aksje"}), 400

    list_path = hd.resolve_list_csv_path(list_name)
    if not list_path:
        return jsonify({"error": f"Fant ikke listefil: {list_name}"}), 404

    t_conn = time.perf_counter()
    conn = hd.db_connect()
    investor_ids = _get_cached_investor_ids(query_key) if query_key else None
    t_ids_start = time.perf_counter()
    if investor_ids is None:
        investor_ids = _load_investor_ids_for_beste_viktige(conn, list_name, date_from, date_to)
    t_ids = time.perf_counter()

    if not investor_ids:
        conn.close()
        return jsonify({"rows": [], "message": "Fant ingen investorer som matcher listen"})

    dfi = hd.fetch_best_viktige_trades_for_isin(conn, investor_ids, isin, date_from, date_to)
    t_details = time.perf_counter()
    conn.close()

    if dfi.empty:
        _LOG.info("beste-viktige details timing list=%s isin=%s conn=%.3fs ids=%.3fs details=%.3fs total=%.3fs (empty)", list_name, isin, t_ids_start-t_conn, t_ids-t_ids_start, t_details-t_ids, t_details-t0)
        return jsonify({"rows": []})

    detail_cols = ["eier", "investor_id", "investor_type", "antall_obs", "kjop_mnok", "salg_mnok", "netto_mnok"]
    for c in ["kjop_mnok", "salg_mnok", "netto_mnok"]:
        dfi[c] = pd.to_numeric(dfi[c], errors="coerce").round(4)
    t_end = time.perf_counter()
    _LOG.info("beste-viktige details timing list=%s isin=%s conn=%.3fs ids=%.3fs details=%.3fs format=%.3fs total=%.3fs rows=%d", list_name, isin, t_ids_start-t_conn, t_ids-t_ids_start, t_details-t_ids, t_end-t_details, t_end-t0, len(dfi))
    resp = make_response(jsonify({"rows": dfi[detail_cols].to_dict("records")}))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@handler_bp.route("/api/beste-viktige/list")
def api_beste_viktige_get_list():
    list_name = _normalize_list_filename(request.args.get("list_name"))
    try:
        content = _read_list_file_content(list_name)
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify({"list_name": list_name, "content": content})


@handler_bp.route("/api/beste-viktige/list/save", methods=["POST"])
def api_beste_viktige_save_list():
    payload = request.get_json(silent=True) or {}
    list_name = _normalize_list_filename(payload.get("list_name"))
    save_as = _normalize_list_filename(payload.get("save_as") or list_name)
    content = str(payload.get("content") or "")
    if not content.strip():
        return jsonify({"error": "Listeinnhold kan ikke være tomt"}), 400
    try:
        saved_path = _write_list_file_content(save_as, content)
    except Exception as exc:
        return jsonify({"error": f"Klarte ikke lagre listefil: {exc}"}), 500
    return jsonify({"ok": True, "saved_as": save_as, "path": saved_path})


# =========================================================
# 5) Beste investorer
# =========================================================
@handler_bp.route("/beste-investorer")
def beste_investorer_page():
    csv_files = hd.list_csv_files()
    err = _check_db()
    country_codes = []
    investor_types = []
    if not err:
        conn = hd.db_connect()
        cols = hdb.detect_cols(conn)
        country_codes = hdb.fetch_distinct_country_codes(conn, cols)
        investor_types = ["Alle", "Organisasjon", "Privat"]
        conn.close()
    return render_template("handler/beste_investorer.html",
                           csv_files=csv_files, country_codes=country_codes,
                           investor_types=investor_types, db_error=err)


@handler_bp.route("/api/beste-investorer/search-investors")
def api_beste_search_investors():
    q = request.args.get("q", "")
    country = request.args.get("country", "")
    err = _check_db()
    if err:
        return jsonify({"error": err}), 503
    conn = hd.db_connect()
    cols = hdb.detect_cols(conn)
    cf = [c.strip() for c in country.split(",") if c.strip()] or None
    results = hdb.search_investors_advanced(conn, cols, q, country_filter=cf)
    conn.close()
    return jsonify(results)


@handler_bp.route("/api/beste-investorer/search-securities")
def api_beste_search_securities():
    q = request.args.get("q", "")
    err = _check_db()
    if err:
        return jsonify({"error": err}), 503
    conn = hd.db_connect()
    cols = hdb.detect_cols(conn)
    results = hdb.search_securities_by_prefix(conn, cols, q)
    conn.close()
    return jsonify(results)


@handler_bp.route("/api/beste-investorer/run")
def api_beste_investorer_run():
    date_from = _parse_date(request.args.get("date_from"), dt.date(2022, 1, 1))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    invtype = request.args.get("invtype", "Alle")
    min_trades = int(request.args.get("min_trades", 10))
    min_brutto = float(request.args.get("min_brutto_mnok", 10.0))

    # Investor filter
    inv_mode = request.args.get("inv_mode", "alle")
    investor_ids = None
    if inv_mode == "csv":
        csv_file = request.args.get("csv_file", "")
        if csv_file:
            path = hd.resolve_list_csv_path(csv_file)
            if path:
                investor_ids = hdb.load_first_column_values(path)
    elif inv_mode == "selected":
        ids_str = request.args.get("investor_ids", "")
        if ids_str:
            investor_ids = [x.strip() for x in ids_str.split(",") if x.strip()]

    # Country filter
    country_str = request.args.get("country_codes", "")
    country_codes = [c.strip() for c in country_str.split(",") if c.strip()] or None

    # ISIN filter
    sec_mode = request.args.get("sec_mode", "alle")
    isin_filter = None
    if sec_mode == "single":
        isin = request.args.get("isin", "").strip()
        if isin:
            isin_filter = [isin]
    elif sec_mode == "csv":
        csv_file = request.args.get("sec_csv_file", "")
        if csv_file:
            path = hd.resolve_list_csv_path(csv_file)
            if path:
                conn_tmp = hd.db_connect()
                cols_tmp = hdb.detect_cols(conn_tmp)
                tokens = hdb.load_first_column_values(path)
                isin_filter = hdb.resolve_isin_list_by_prefix(conn_tmp, cols_tmp, tokens) or None
                conn_tmp.close()

    err = _check_db()
    if err:
        return jsonify({"error": err}), 503

    conn = hd.db_connect()
    cols = hdb.detect_cols(conn)
    hdb.ensure_indexes(conn, cols)

    last_map, source_txt = hdb.build_last_price_cache(conn, cols)

    res = hdb.compute_best_investors(
        conn=conn, cols=cols,
        date_from=date_from, date_to=date_to,
        investor_ids=investor_ids, invtype_choice=invtype,
        country_codes=country_codes, isin_filter=isin_filter,
        min_trades=min_trades, min_brutto_mnok=min_brutto,
        last_price_map=last_map,
    )
    conn.close()

    if res.empty:
        return jsonify({"rows": [], "price_source": source_txt, "message": "Ingen treff"})

    for c in ["brutto_mnok", "gevinst_mnok", "gevinst_pct"]:
        if c in res.columns:
            res[c] = res[c].round(2)

    return jsonify({
        "rows": res.to_dict("records"),
        "price_source": source_txt,
        "count": len(res),
    })


@handler_bp.route("/api/beste-investorer/transactions")
def api_beste_transactions():
    investor_id = request.args.get("investor_id", "").strip()
    date_from = _parse_date(request.args.get("date_from"), dt.date(2022, 1, 1))
    date_to = _parse_date(request.args.get("date_to"), dt.date.today())
    isin_str = request.args.get("isin_filter", "")
    isin_filter = [x.strip() for x in isin_str.split(",") if x.strip()] or None

    if not investor_id:
        return jsonify({"error": "Mangler investor_id"}), 400

    cache_key = _cache_key_for_beste_transactions(investor_id, date_from, date_to, isin_filter)
    cached_payload = _get_cached_beste_transactions(cache_key)
    if cached_payload is not None:
        return jsonify(cached_payload)

    conn = hd.db_connect()
    cols = hdb.detect_cols(conn)
    last_map, _ = hdb.build_last_price_cache(conn, cols)

    tx = hdb.fetch_transactions(conn, cols, investor_id, date_from, date_to, isin_filter, last_map)
    conn.close()

    if tx.empty:
        payload = {"rows": [], "totals": {}}
        _set_cached_beste_transactions(cache_key, payload)
        return jsonify(payload)

    for c in ["brutto_mnok", "gevinst_mnok"]:
        if c in tx.columns:
            tx[c] = tx[c].round(2)

    show = ["dato", "ticker", "navn", "isin", "qty", "trade_price", "last_price", "brutto_mnok", "gevinst_mnok"]
    show = [c for c in show if c in tx.columns]

    gross_total = tx["gross_kr"].sum() / 1_000_000
    profit_total = tx["profit_kr"].sum() / 1_000_000
    pct_total = (tx["profit_kr"].sum() / tx["gross_kr"].sum() * 100.0) if tx["gross_kr"].sum() else 0.0

    payload = {
        "rows": tx[show].to_dict("records"),
        "totals": {
            "brutto_mnok": round(gross_total, 2),
            "gevinst_mnok": round(profit_total, 2),
            "gevinst_pct": round(pct_total, 2),
        },
    }
    _set_cached_beste_transactions(cache_key, payload)
    return jsonify(payload)

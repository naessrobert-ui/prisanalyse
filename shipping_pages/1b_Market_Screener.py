from __future__ import annotations

from pathlib import Path
import json
import re

import pandas as pd
import streamlit as st

from shipping_datahandling._6_Datahandling_Market_Screener import (
    add_tickers_from_input,
    build_screener_universe,
    custom_tickers_signature,
    remove_ticker_from_screener,
)
from shipping_app.data import get_company_names, get_company_profile
from shipping_app.ui import render_sidebar


st.set_page_config(
    page_title="Stock Screener",
    page_icon="SS",
    layout="wide",
    initial_sidebar_state="expanded",
)

render_sidebar()


_SHIPPING_BASE = Path(__file__).resolve().parent.parent / "shipping_app"
TRADING_SIGNALS_DIR = _SHIPPING_BASE / "Output" / "Trading_Algorithm"
FAVORITES_PATH = _SHIPPING_BASE / "Output" / "Market_Screener" / "favorites.json"
COMPANY_PROFILES_PATH = _SHIPPING_BASE / "Data" / "company_profiles.json"


def _normalize_signal_label(value: object) -> str:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return "-"
    # Remove optional horizon suffix from legacy labels like "Buy (3m)".
    return re.sub(r"\s*\(\d+m\)\s*$", "", text, flags=re.IGNORECASE)


def _trading_signal_files_signature() -> tuple[tuple[str, int], ...]:
    if not TRADING_SIGNALS_DIR.exists():
        return ()

    signal_files = sorted(
        TRADING_SIGNALS_DIR.glob("*_latest_trading_signals.csv"),
        key=lambda file_path: file_path.name,
    )
    return tuple((file_path.name, file_path.stat().st_mtime_ns) for file_path in signal_files)


def _company_profiles_signature() -> tuple[str, int]:
    if not COMPANY_PROFILES_PATH.exists():
        return ("missing", 0)
    return (COMPANY_PROFILES_PATH.name, COMPANY_PROFILES_PATH.stat().st_mtime_ns)


def _load_latest_signal_snapshot_by_ticker() -> dict[str, dict[str, dict[str, object]]]:
    if not TRADING_SIGNALS_DIR.exists():
        return {}

    profiles = [get_company_profile(company) for company in get_company_names()]
    safe_company_to_ticker = {
        profile.name.lower().replace(" ", "_"): profile.ticker
        for profile in profiles
    }
    company_name_to_ticker = {
        profile.name.lower(): profile.ticker
        for profile in profiles
    }

    latest_by_ticker: dict[str, dict[str, dict[str, object]]] = {}
    for file_path in TRADING_SIGNALS_DIR.glob("*_latest_trading_signals.csv"):
        if file_path.name == "latest_trading_signals.csv":
            continue

        file_stem = file_path.stem.replace("_latest_trading_signals", "")
        file_ticker = safe_company_to_ticker.get(file_stem)
        if not file_ticker:
            continue

        try:
            signal_df = pd.read_csv(file_path)
        except Exception:
            continue

        required_cols = {"horizon", "action", "implicit_rate", "spread"}
        if signal_df.empty or not required_cols.issubset(set(signal_df.columns)):
            continue

        if "company" in signal_df.columns:
            company_ticker = company_name_to_ticker.get(str(signal_df["company"].iloc[0]).strip().lower())
            ticker = company_ticker or file_ticker
        else:
            ticker = file_ticker

        signal_dates = pd.to_datetime(signal_df.get("date"), errors="coerce")
        if signal_dates.isna().all() and "row_date" in signal_df.columns:
            signal_dates = pd.to_datetime(signal_df["row_date"], errors="coerce")

        signal_df = signal_df.assign(_signal_date=signal_dates)
        ticker_payload = latest_by_ticker.setdefault(ticker, {})
        for _, horizon_row in signal_df.iterrows():
            horizon = str(horizon_row.get("horizon", "")).strip().lower()
            if horizon not in {"3m", "6m", "12m"}:
                continue

            signal_date = horizon_row.get("_signal_date")
            signal_date = signal_date if isinstance(signal_date, pd.Timestamp) else pd.NaT
            action = _normalize_signal_label(horizon_row.get("action", "-"))
            implicit_rate = pd.to_numeric(horizon_row.get("implicit_rate"), errors="coerce")
            spread = pd.to_numeric(horizon_row.get("spread"), errors="coerce")

            current = ticker_payload.get(horizon)
            if current is not None:
                current_date = current.get("_signal_date", pd.NaT)
                if pd.notna(current_date) and pd.notna(signal_date) and current_date >= signal_date:
                    continue

            ticker_payload[horizon] = {
                "_signal_date": signal_date,
                "action": action,
                "implicit_rate": float(implicit_rate) if pd.notna(implicit_rate) else 0.0,
                "spread": float(spread) if pd.notna(spread) else 0.0,
            }

    return latest_by_ticker


@st.cache_data(show_spinner=False)
def _load_screener_data(
    _signal_files_signature: tuple[tuple[str, int], ...],
    _custom_tickers_signature: tuple[str, int],
    _company_profiles_signature: tuple[str, int],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    latest_signals = _load_latest_signal_snapshot_by_ticker()
    return build_screener_universe(latest_signals)


def _filter_stocks(stocks: list[dict[str, object]], filters: dict[str, object]) -> pd.DataFrame:
    frame = pd.DataFrame(stocks)
    if frame.empty:
        return frame

    filtered = frame.loc[
        (frame["market_cap_usd_bn"] >= filters["market_cap_min"])
        & (frame["market_cap_usd_bn"] <= filters["market_cap_max"])
        & (frame["stock_price"] >= filters["price_min"])
        & (frame["stock_price"] <= filters["price_max"])
    ].copy()

    signal_filter = str(filters.get("signal", "Any")).strip().lower()
    if signal_filter != "any":
        signal_cols = ["signal_3m", "signal_6m", "signal_12m"]
        for signal_col in signal_cols:
            if signal_col not in filtered.columns:
                filtered[signal_col] = "-"
        mask = filtered[signal_cols].apply(
            lambda row: any(str(value).strip().lower().startswith(signal_filter) for value in row),
            axis=1,
        )
        filtered = filtered.loc[mask].copy()

    return filtered


def _format_results_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    table = df.copy()
    if "latest_signal" in table.columns:
        table["latest_signal"] = table["latest_signal"].map(_normalize_signal_label)
    table["52W High/Low"] = table.apply(
        lambda row: f"${row['high_52w']:,.2f} / ${row['low_52w']:,.2f}",
        axis=1,
    )
    table = table.rename(
        columns={
            "ticker": "Ticker",
            "company_name": "Company Name",
            "market_cap_usd_bn": "Market Cap",
            "pe_ratio": "P/E Ratio",
            "dividend_yield_pct": "Dividend Yield (%)",
            "day_rate_trend": "Day Rate Trend",
            "latest_signal": "Latest Signal",
            "score": "Score",
        }
    )
    return table[
        [
            "Ticker",
            "Company Name",
            "Market Cap",
            "P/E Ratio",
            "Dividend Yield (%)",
            "Day Rate Trend",
            "Latest Signal",
            "52W High/Low",
            "Score",
        ]
    ]


def _highlight_top_scores(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    top_three_scores = set(df.nlargest(3, "Score")["Score"].tolist())

    def style_rows(row: pd.Series) -> list[str]:
        base_style = "background-color: #f0f9ff;" if row["Score"] in top_three_scores else ""
        styles = [base_style] * len(row)

        if "Latest Signal" in row.index:
            signal_style_base = (
                "font-family: inherit; font-weight: 700; "
                "-webkit-text-stroke: 1.0px #000; "
                "text-shadow: "
                "-2px 0 #000, 2px 0 #000, 0 -2px #000, 0 2px #000, "
                "-2px -2px #000, -2px 2px #000, 2px -2px #000, 2px 2px #000;"
            )
            signal_value = str(row["Latest Signal"]).strip().lower()
            if signal_value.startswith("buy"):
                styles[df.columns.get_loc("Latest Signal")] = (
                    f"{base_style} color: #0f9d58; {signal_style_base}"
                ).strip()
            elif signal_value.startswith("sell"):
                styles[df.columns.get_loc("Latest Signal")] = (
                    f"{base_style} color: #d93025; {signal_style_base}"
                ).strip()
            elif signal_value.startswith("hold"):
                styles[df.columns.get_loc("Latest Signal")] = (
                    f"{base_style} color: #1a73e8; {signal_style_base}"
                ).strip()
            elif signal_value != "-":
                styles[df.columns.get_loc("Latest Signal")] = (
                    f"{base_style} color: #202124; {signal_style_base}"
                ).strip()

        return styles

    return df.style.apply(style_rows, axis=1)


def _reset_filters() -> None:
    st.session_state["screener_market_cap"] = st.session_state.get(
        "screener_market_cap_default",
        (0.0, 50.0),
    )
    st.session_state["screener_price"] = st.session_state.get(
        "screener_price_default",
        (0.0, 200.0),
    )
    st.session_state["screener_signal"] = "Any"
    st.session_state["screener_show_favorites_only"] = False


def _initialize_filters_for_universe(stocks: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(stocks)
    if frame.empty:
        return

    market_cap = pd.to_numeric(frame.get("market_cap_usd_bn"), errors="coerce")
    stock_price = pd.to_numeric(frame.get("stock_price"), errors="coerce")
    market_cap_max = float(market_cap.max()) if not market_cap.dropna().empty else 10.0
    stock_price_max = float(stock_price.max()) if not stock_price.dropna().empty else 50.0
    full_market_cap = (0.0, max(10.0, round(market_cap_max + 1.0, 2)))
    full_price = (0.0, max(50.0, round(stock_price_max + 1.0, 2)))

    signature = (
        len(stocks),
        round(full_market_cap[1], 2),
        round(full_price[1], 2),
    )
    previous_signature = st.session_state.get("screener_universe_signature")

    # Reinitialize filters whenever the universe changes so all companies remain visible by default.
    if previous_signature != signature:
        st.session_state["screener_market_cap_default"] = full_market_cap
        st.session_state["screener_price_default"] = full_price
        st.session_state["screener_market_cap"] = full_market_cap
        st.session_state["screener_price"] = full_price
        st.session_state["screener_signal"] = "Any"
        st.session_state["screener_show_favorites_only"] = False
        st.session_state["screener_universe_signature"] = signature


def _clamp_range_filter_state(key: str, *, upper_bound: float, fallback_upper: float) -> tuple[float, float]:
    current_range = st.session_state.get(key, (0.0, fallback_upper))
    clamped_range = (
        max(0.0, min(float(current_range[0]), upper_bound)),
        max(0.0, min(float(current_range[1]), upper_bound)),
    )
    if clamped_range[0] > clamped_range[1]:
        clamped_range = (0.0, upper_bound)
    st.session_state[key] = clamped_range
    return clamped_range


def _init_favorites_state() -> None:
    if st.session_state.get("screener_favorites_loaded", False):
        return

    favorites: list[str] = []
    if FAVORITES_PATH.exists():
        try:
            payload = json.loads(FAVORITES_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                favorites = [str(item).strip().upper() for item in payload if str(item).strip()]
        except Exception:
            favorites = []

    st.session_state["screener_favorites"] = sorted(set(favorites))
    st.session_state["screener_favorites_loaded"] = True


def _persist_favorites(favorites: set[str]) -> None:
    FAVORITES_PATH.parent.mkdir(parents=True, exist_ok=True)
    FAVORITES_PATH.write_text(
        json.dumps(sorted(favorites), indent=2),
        encoding="utf-8",
    )


def _toggle_favorite_ticker(ticker: str) -> None:
    favorites = set(st.session_state.get("screener_favorites", []))
    if ticker in favorites:
        favorites.remove(ticker)
    else:
        favorites.add(ticker)
    st.session_state["screener_favorites"] = sorted(favorites)
    _persist_favorites(favorites)


def _add_tickers_and_optionally_favorite(raw_tickers: str, add_to_favorites: bool) -> dict[str, list[str]]:
    result = add_tickers_from_input(raw_tickers)
    tickers_to_favorite = result.get("added", []) + result.get("already_present", [])
    if add_to_favorites and tickers_to_favorite:
        favorites = set(st.session_state.get("screener_favorites", []))
        favorites.update(tickers_to_favorite)
        st.session_state["screener_favorites"] = sorted(favorites)
        _persist_favorites(favorites)
    return result


def _remove_ticker_and_cleanup(ticker: str) -> dict[str, str]:
    try:
        removed_company_name = remove_ticker_from_screener(ticker)
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

    favorites = set(st.session_state.get("screener_favorites", []))
    if ticker in favorites:
        favorites.remove(ticker)
        st.session_state["screener_favorites"] = sorted(favorites)
        _persist_favorites(favorites)

    return {
        "status": "success",
        "message": f"Removed {ticker} ({removed_company_name}) from company profiles.",
    }


def _request_remove_company(ticker: str, company_name: str) -> None:
    st.session_state["screener_pending_remove"] = {
        "ticker": ticker,
        "company_name": company_name,
    }


def _render_remove_confirmation_content() -> None:
    pending_remove = st.session_state.get("screener_pending_remove")
    if not pending_remove:
        return

    pending_ticker = str(pending_remove.get("ticker", "")).strip()
    pending_company_name = str(pending_remove.get("company_name", pending_ticker)).strip()
    st.warning(f"Are you sure you want to remove {pending_company_name}?")
    confirm_col, cancel_col = st.columns(2, gap="small")
    with confirm_col:
        if st.button("Yes, remove", key="screener_confirm_remove_button", width="stretch"):
            result = _remove_ticker_and_cleanup(pending_ticker)
            st.session_state["screener_remove_feedback"] = result
            st.session_state.pop("screener_pending_remove", None)
            _load_screener_data.clear()
            st.rerun()
    with cancel_col:
        if st.button("Cancel", key="screener_cancel_remove_button", width="stretch"):
            st.session_state.pop("screener_pending_remove", None)
            st.rerun()


if hasattr(st, "dialog"):
    _render_remove_confirmation_dialog = st.dialog("Confirm removal")(_render_remove_confirmation_content)
else:
    _render_remove_confirmation_dialog = None


def _signal_card_style(signal_value: str) -> tuple[str, str]:
    signal = _normalize_signal_label(signal_value).lower()
    base_outline = "font-weight: 700;"
    if signal.startswith("buy"):
        return "#0f9d58", base_outline
    if signal.startswith("sell"):
        return "#d93025", base_outline
    if signal.startswith("hold"):
        return "#1a73e8", base_outline
    return "#202124", "font-weight: 700;"


def _favorite_signal_html(signal_value: str) -> str:
    label = _normalize_signal_label(signal_value)
    color, extra_style = _signal_card_style(label)
    return (
        f"<div style='font-size: 2.1rem; font-weight: 800; color: {color}; "
        f"margin-bottom: 0.15rem; line-height: 1; {extra_style}'>{label}</div>"
    )


def _format_implicit_display(stock: dict[str, object]) -> str:
    implicit_vlcc = float(stock.get("implicit_rate_vlcc", 0.0))
    implicit_suezmax = float(stock.get("implicit_rate_suezmax", 0.0))
    implicit_aframax_lr2 = float(stock.get("implicit_rate_aframax_lr2", 0.0))
    implicit_mr = float(stock.get("implicit_rate_mr", 0.0))
    return f"${implicit_vlcc:,.0f}/${implicit_suezmax:,.0f}/${implicit_aframax_lr2:,.0f}/${implicit_mr:,.0f}"


def _format_diff_display(stock: dict[str, object]) -> str:
    diff_3m = float(stock.get("diff_3m", 0.0))
    diff_6m = float(stock.get("diff_6m", 0.0))
    diff_12m = float(stock.get("diff_12m", 0.0))
    return f"{diff_3m:+,.0f}/{diff_6m:+,.0f}/{diff_12m:+,.0f}"


def _signal_cell_html(signal_value: str) -> str:
    color, style = _signal_card_style(signal_value)
    return f"<span style='color:{color}; {style}'>{signal_value}</span>"


def _signal_triplet_html(signal_3m: str, signal_6m: str, signal_12m: str) -> str:
    color_3m, style_3m = _signal_card_style(signal_3m)
    color_6m, style_6m = _signal_card_style(signal_6m)
    color_12m, style_12m = _signal_card_style(signal_12m)
    return (
        f"<span style='color:{color_3m}; {style_3m}'>{signal_3m}</span>"
        "<span style='color:#64748b;'>/</span>"
        f"<span style='color:{color_6m}; {style_6m}'>{signal_6m}</span>"
        "<span style='color:#64748b;'>/</span>"
        f"<span style='color:{color_12m}; {style_12m}'>{signal_12m}</span>"
    )


def _render_screener_chrome(metadata: dict[str, object], total_stocks: int, favorite_count: int) -> None:
    st.markdown(
        f"""
        <style>
        .block-container {{
            padding-top: 4.5rem;
            padding-bottom: 1.5rem;
        }}
        /* Tighten horizontal spacing so the screener row columns fit better. */
        div[data-testid="stHorizontalBlock"] {{
            gap: 0.2rem;
        }}
        div[data-testid="stColumn"] {{
            padding-left: 0 !important;
            padding-right: 0 !important;
        }}
        div[data-testid="column"] {{
            padding-left: 0 !important;
            padding-right: 0 !important;
        }}
        .screener-hero {{
            border-radius: 26px;
            padding: 0.95rem 1.05rem 0.8rem 1.05rem;
            background:
                radial-gradient(circle at top right, rgba(245, 158, 11, 0.24), transparent 24%),
                linear-gradient(135deg, #0f172a 0%, #1e293b 55%, #334155 100%);
            color: white;
            margin: 0.2rem 0 0.75rem 0;
            box-shadow: 0 18px 36px rgba(15, 23, 42, 0.18);
            border: 1px solid rgba(255, 255, 255, 0.06);
        }}
        .screener-hero-title {{
            font-size: 1.8rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            margin-bottom: 0.22rem;
        }}
        .screener-hero-copy {{
            font-size: 0.9rem;
            color: #dbe4ee;
            margin-bottom: 0.75rem;
            max-width: 62rem;
        }}
        .screener-hero-grid {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.65rem;
        }}
        .screener-hero-stat {{
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.10);
            padding: 0.58rem 0.72rem;
            backdrop-filter: blur(8px);
        }}
        .screener-hero-label {{
            font-size: 0.76rem;
            color: #cbd5e1;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.18rem;
        }}
        .screener-hero-value {{
            font-size: 1.15rem;
            font-weight: 800;
            color: white;
        }}
        .screener-section-kicker {{
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            padding: 0.28rem 0.65rem;
            border-radius: 999px;
            background: #eef4ff;
            border: 1px solid #dbe7ff;
            color: #1d4ed8;
            font-size: 0.75rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.55rem;
        }}
        .screener-favorite-card {{
            border-radius: 22px;
            padding: 0.8rem 0.9rem 0.78rem 0.9rem;
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #dbe4ee;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
            min-height: 168px;
        }}
        .screener-favorite-topline {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 0.45rem;
        }}
        .screener-favorite-ticker {{
            font-size: 1.35rem;
            font-weight: 800;
            color: #0f172a;
            line-height: 1;
        }}
        .screener-favorite-company {{
            font-size: 0.92rem;
            color: #64748b;
            margin-top: 0.16rem;
        }}
        .screener-favorite-score {{
            padding: 0.35rem 0.65rem;
            border-radius: 999px;
            background: #e2e8f0;
            color: #334155;
            font-weight: 800;
            font-size: 0.82rem;
            border: 1px solid #cbd5e1;
        }}
        .screener-favorite-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.5rem 0.8rem;
            margin-top: 0.45rem;
        }}
        .screener-favorite-metric-label {{
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748b;
            margin-bottom: 0.15rem;
        }}
        .screener-favorite-metric-value {{
            font-size: 0.96rem;
            font-weight: 700;
            color: #0f172a;
        }}
        .screener-row-header {{
            font-size: 0.75rem;
            font-weight: 800;
            color: #475569;
            line-height: 1.2;
            margin: 0;
            white-space: nowrap;
            display: flex;
            align-items: center;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}
        .screener-row-cell {{
            font-size: 0.81rem;
            line-height: 1.2;
            color: #334155;
            margin: 0;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: flex;
            align-items: center;
        }}
        .screener-row-shell {{
            border-radius: 18px;
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #dbe4ee;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
            padding: 0.14rem 0.14rem;
        }}
        .screener-row-grid,
        .screener-row-header-grid {{
            display: grid;
            grid-template-columns: 0.8fr 1.1fr 0.8fr 0.8fr 0.8fr 0.8fr 0.9fr 0.7fr 0.7fr 0.72fr 0.72fr 0.72fr 0.8fr 0.8fr 0.8fr;
            column-gap: 0.18rem;
            align-items: center;
            width: 100%;
        }}
        .screener-row-group-grid {{
            display: grid;
            grid-template-columns: 0.8fr 1.1fr 0.8fr 0.8fr 0.8fr 0.8fr 0.9fr 0.7fr 0.7fr 0.72fr 0.72fr 0.72fr 0.8fr 0.8fr 0.8fr;
            column-gap: 0.18rem;
            align-items: end;
            width: 100%;
            margin-bottom: 0.08rem;
        }}
        .screener-row-group-header {{
            font-size: 0.67rem;
            font-weight: 800;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            white-space: nowrap;
        }}
        .screener-row-shell.featured {{
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border-color: #dbe4ee;
        }}
        .screener-refresh-wrap {{
            margin-bottom: 0.55rem;
        }}
        .screener-filter-shell {{
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
        }}
        .screener-filter-card-offset {{
            margin-top: 0;
        }}
        .screener-refresh-wrap .st-key-screener_refresh_button button,
        .screener-refresh-wrap [data-testid="stPopover"] button {{
            min-height: 3.15rem;
            font-size: 0.98rem;
            font-weight: 700;
            border-radius: 14px;
        }}
        [class*="st-key-screener_row_fav_"] button,
        [class*="st-key-screener_feature_fav_"] button {{
            font-size: 0.42rem;
            padding-top: 0;
            padding-bottom: 0;
            padding-left: 0.05rem;
            padding-right: 0.05rem;
            min-height: 1.1rem;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }}
        [class*="st-key-screener_row_remove_"] button {{
            font-size: 0.72rem;
            padding-top: 0;
            padding-bottom: 0;
            padding-left: 0.1rem;
            padding-right: 0.1rem;
            min-height: 1.5rem;
            border-radius: 10px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }}
        </style>
        <div class="screener-hero">
            <div class="screener-hero-title">Market Screener</div>
            <div class="screener-hero-copy">Scan the shipping universe, pin your conviction names, and compare live signal posture against a compact fundamentals snapshot.</div>
            <div class="screener-hero-grid">
                <div class="screener-hero-stat">
                    <div class="screener-hero-label">Universe</div>
                    <div class="screener-hero-value">{total_stocks}</div>
                </div>
                <div class="screener-hero-stat">
                    <div class="screener-hero-label">Favorites</div>
                    <div class="screener-hero-value">{favorite_count}</div>
                </div>
                <div class="screener-hero-stat">
                    <div class="screener-hero-label">Signals Source</div>
                    <div class="screener-hero-value" style="font-size: 1.0rem;">Trading Model</div>
                </div>
                <div class="screener-hero-stat">
                    <div class="screener-hero-label">Snapshot</div>
                    <div class="screener-hero-value" style="font-size: 1.0rem;">{metadata.get('generated_at_utc', 'Unknown')}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_favorite_feature_cards(stocks_df: pd.DataFrame, *, card_prefix: str, favorite_tickers: set[str]) -> None:
    if stocks_df.empty:
        return

    cards = stocks_df.to_dict("records")
    cols = st.columns(2, gap="medium")
    for idx, stock in enumerate(cards):
        ticker = str(stock.get("ticker", ""))
        signal_3m = _normalize_signal_label(stock.get("signal_3m", "-"))
        signal_6m = _normalize_signal_label(stock.get("signal_6m", "-"))
        signal_12m = _normalize_signal_label(stock.get("signal_12m", "-"))
        implicit_display = _format_implicit_display(stock)
        diff_display = _format_diff_display(stock)
        signal_triplet = _signal_triplet_html(signal_3m, signal_6m, signal_12m)
        with cols[idx % 2]:
            with st.container(border=False):
                outer_cols = st.columns([0.12, 0.88], gap="small")
                with outer_cols[0]:
                    star_label = "⭐" if ticker in favorite_tickers else "☆"
                    if st.button(
                        star_label,
                        key=f"screener_feature_fav_{card_prefix}_{ticker}_{idx}",
                    width="stretch",
                    ):
                        _toggle_favorite_ticker(ticker)
                        st.rerun()
                with outer_cols[1]:
                    st.markdown(
                        f"""
                        <div class="screener-favorite-card">
                            <div class="screener-favorite-topline">
                                <div>
                                    <div class="screener-favorite-ticker">{ticker}</div>
                                    <div class="screener-favorite-company">{stock.get('company_name', 'Unknown')}</div>
                                </div>
                                <div class="screener-favorite-score">${float(stock.get('stock_price', 0.0)):.2f}</div>
                            </div>
                            <div class="screener-favorite-grid">
                                <div>
                                    <div class="screener-favorite-metric-label">Market Cap</div>
                                    <div class="screener-favorite-metric-value">${float(stock.get('market_cap_usd_bn', 0.0)):.2f}B</div>
                                </div>
                                <div>
                                    <div class="screener-favorite-metric-label">Implicit (VLCC/Suezmax/Aframax-LR2/MR)</div>
                                    <div class="screener-favorite-metric-value">{implicit_display}</div>
                                </div>
                                <div>
                                    <div class="screener-favorite-metric-label">Signal (3m / 6m / 12m)</div>
                                    <div class="screener-favorite-metric-value">{signal_triplet}</div>
                                </div>
                                <div>
                                    <div class="screener-favorite-metric-label">Diff (3m / 6m / 12m)</div>
                                    <div class="screener-favorite-metric-value">{diff_display}</div>
                                </div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )


def _render_row_cards(stocks_df: pd.DataFrame, *, card_prefix: str, favorite_tickers: set[str]) -> None:
    if stocks_df.empty:
        return

    header_left, header_mid, header_right = st.columns([0.06, 0.88, 0.06], gap="small")
    with header_mid:
        st.markdown(
            """
            <div class='screener-row-group-grid'>
                <div class='screener-row-group-header' style='grid-column: 1 / span 1;'>Ticker</div>
                <div class='screener-row-group-header' style='grid-column: 2 / span 1;'>Company</div>
                <div class='screener-row-group-header' style='grid-column: 3 / span 1;'>Price</div>
                <div class='screener-row-group-header' style='grid-column: 4 / span 1;'>MCap</div>
                <div class='screener-row-group-header' style='grid-column: 5 / span 4;'>Implicit</div>
                <div class='screener-row-group-header' style='grid-column: 9 / span 3;'>Signal</div>
                <div class='screener-row-group-header' style='grid-column: 12 / span 3;'>Diff</div>
            </div>
            <div class='screener-row-header-grid'>
                <div class='screener-row-header'>Ticker</div>
                <div class='screener-row-header'>Company</div>
                <div class='screener-row-header'>Price</div>
                <div class='screener-row-header'>MCap</div>
                <div class='screener-row-header'>VLCC</div>
                <div class='screener-row-header'>Suez</div>
                <div class='screener-row-header'>A/LR2</div>
                <div class='screener-row-header'>MR</div>
                <div class='screener-row-header'>3m</div>
                <div class='screener-row-header'>6m</div>
                <div class='screener-row-header'>12m</div>
                <div class='screener-row-header'>3m</div>
                <div class='screener-row-header'>6m</div>
                <div class='screener-row-header'>12m</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    card_records = stocks_df.to_dict("records")
    for idx, stock in enumerate(card_records):
        ticker = str(stock.get("ticker", ""))
        company_name = str(stock.get("company_name", "Unknown"))
        signal_3m = _normalize_signal_label(stock.get("signal_3m", "-"))
        signal_6m = _normalize_signal_label(stock.get("signal_6m", "-"))
        signal_12m = _normalize_signal_label(stock.get("signal_12m", "-"))
        implicit_vlcc = float(stock.get("implicit_rate_vlcc", 0.0))
        implicit_suezmax = float(stock.get("implicit_rate_suezmax", 0.0))
        implicit_aframax_lr2 = float(stock.get("implicit_rate_aframax_lr2", 0.0))
        implicit_mr = float(stock.get("implicit_rate_mr", 0.0))
        diff_3m = float(stock.get("diff_3m", 0.0))
        diff_6m = float(stock.get("diff_6m", 0.0))
        diff_12m = float(stock.get("diff_12m", 0.0))
        signal_3m_html = _signal_cell_html(signal_3m)
        signal_6m_html = _signal_cell_html(signal_6m)
        signal_12m_html = _signal_cell_html(signal_12m)

        with st.container(border=False):
            st.markdown("<div class='screener-row-shell'>", unsafe_allow_html=True)
            row_cols = st.columns([0.06, 0.88, 0.06], gap="small")

            with row_cols[0]:
                star_label = "⭐" if ticker in favorite_tickers else "☆"
                if st.button(
                    star_label,
                    key=f"screener_row_fav_{card_prefix}_{ticker}_{idx}",
                        width="stretch",
                ):
                    _toggle_favorite_ticker(ticker)
                    st.rerun()

            with row_cols[1]:
                st.markdown(
                    (
                        "<div class='screener-row-grid'>"
                        f"<div class='screener-row-cell'><strong>{ticker}</strong></div>"
                        f"<div class='screener-row-cell'>{company_name}</div>"
                        f"<div class='screener-row-cell'>${float(stock.get('stock_price', 0.0)):.2f}</div>"
                        f"<div class='screener-row-cell'>${float(stock.get('market_cap_usd_bn', 0.0)):.2f}B</div>"
                        f"<div class='screener-row-cell'>${implicit_vlcc:,.0f}</div>"
                        f"<div class='screener-row-cell'>${implicit_suezmax:,.0f}</div>"
                        f"<div class='screener-row-cell'>${implicit_aframax_lr2:,.0f}</div>"
                        f"<div class='screener-row-cell'>${implicit_mr:,.0f}</div>"
                        f"<div class='screener-row-cell'>{signal_3m_html}</div>"
                        f"<div class='screener-row-cell'>{signal_6m_html}</div>"
                        f"<div class='screener-row-cell'>{signal_12m_html}</div>"
                        f"<div class='screener-row-cell'>{diff_3m:+,.0f}</div>"
                        f"<div class='screener-row-cell'>{diff_6m:+,.0f}</div>"
                        f"<div class='screener-row-cell'>{diff_12m:+,.0f}</div>"
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
            with row_cols[2]:
                if st.button(
                    "x",
                    key=f"screener_row_remove_{card_prefix}_{ticker}_{idx}",
                    width="stretch",
                ):
                    _request_remove_company(ticker, company_name)
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)


is_loading = True
error = None
all_stocks: list[dict[str, object]] = []
metadata: dict[str, object] = {}

try:
    with st.spinner("Loading screener universe..."):
        all_stocks, metadata = _load_screener_data(
            _trading_signal_files_signature(),
            custom_tickers_signature(),
            _company_profiles_signature(),
        )
except Exception as exc:
    error = str(exc)
finally:
    is_loading = False

if is_loading:
    st.info("Loading screener data...")

if error:
    st.error(f"Could not load screener data: {error}")
    st.stop()

if not all_stocks:
    st.warning("No stocks available in the screener universe.")
    st.stop()

_initialize_filters_for_universe(all_stocks)

_init_favorites_state()

favorite_count = len(st.session_state.get("screener_favorites", []))

_render_screener_chrome(metadata, len(all_stocks), favorite_count)

st.caption(
    "Data source: "
    f"{metadata.get('source', 'Unknown')} | "
    f"Universe: {metadata.get('total_stock_count', len(all_stocks))} stocks | "
    f"Snapshot built: {metadata.get('generated_at_utc', 'Unknown')}"
)

st.markdown(
    """
    <style>
    /* Keep dropdown behavior but prevent text input/search in the select box. */
    div[data-baseweb="select"] input {
        caret-color: transparent;
        color: transparent;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

favorite_tickers = set(st.session_state.get("screener_favorites", []))
favorite_df = pd.DataFrame(all_stocks)
favorite_df = favorite_df.loc[favorite_df["ticker"].isin(favorite_tickers)].copy()
favorite_df = favorite_df.sort_values(["market_cap_usd_bn", "ticker"], ascending=[False, True])

favorites_header_col, refresh_col = st.columns([0.72, 0.28], gap="large")
with favorites_header_col:
    st.markdown("<div class='screener-section-kicker'>Pinned Names</div>", unsafe_allow_html=True)
    st.subheader("Favorite Stocks")

with refresh_col:
    st.markdown("<div class='screener-refresh-wrap'>", unsafe_allow_html=True)
    add_col, refresh_button_col = st.columns([1, 1], gap="small")
    with add_col:
        with st.popover("Add", width="stretch"):
            add_input = st.text_area(
                "Tickers",
                key="screener_add_tickers_input",
                placeholder="FRO, STNG, GOGL",
                help="Enter one or more tickers separated by commas.",
                height=80,
            )
            add_to_favorites = st.checkbox(
                "Add entered tickers to favorites",
                key="screener_add_to_favorites",
                value=False,
            )
            if st.button("Add tickers", key="screener_add_submit", width="stretch"):
                result = _add_tickers_and_optionally_favorite(add_input, add_to_favorites)
                messages: list[str] = []
                if result["added"]:
                    messages.append(f"Added: {', '.join(result['added'])}")
                if result["already_present"]:
                    messages.append(f"Already present: {', '.join(result['already_present'])}")
                if result["invalid"]:
                    messages.append(f"Invalid or unavailable: {', '.join(result['invalid'])}")

                st.session_state["screener_add_feedback"] = {
                    "added_count": len(result["added"]),
                    "message": " | ".join(messages) if messages else "No valid tickers provided.",
                }
                _load_screener_data.clear()
                st.rerun()

    with refresh_button_col:
        if st.button("Refresh", key="screener_refresh_button", width="stretch"):
            _load_screener_data.clear()
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

feedback = st.session_state.pop("screener_add_feedback", None)
if feedback:
    if feedback.get("added_count", 0) > 0:
        st.success(str(feedback.get("message", "Tickers updated.")))
    else:
        st.warning(str(feedback.get("message", "No tickers were added.")))

remove_feedback = st.session_state.pop("screener_remove_feedback", None)
if remove_feedback:
    if remove_feedback.get("status") == "success":
        st.success(str(remove_feedback.get("message", "Ticker removed.")))
    else:
        st.warning(str(remove_feedback.get("message", "Could not remove ticker.")))

pending_remove = st.session_state.get("screener_pending_remove")
if pending_remove:
    if _render_remove_confirmation_dialog is not None:
        _render_remove_confirmation_dialog()
    else:
        _render_remove_confirmation_content()

if favorite_df.empty:
    st.info("No favorites selected yet.")
else:
    _render_favorite_feature_cards(favorite_df, card_prefix="favorites", favorite_tickers=favorite_tickers)

st.divider()

market_cap_series = pd.to_numeric(
    pd.DataFrame(all_stocks).get("market_cap_usd_bn", pd.Series(dtype=float)),
    errors="coerce",
)
price_series = pd.to_numeric(
    pd.DataFrame(all_stocks).get("stock_price", pd.Series(dtype=float)),
    errors="coerce",
)
market_cap_upper = float(market_cap_series.max()) if not market_cap_series.dropna().empty else 10.0
market_cap_upper = max(10.0, round(market_cap_upper + 1.0, 2))
price_upper = float(price_series.max()) if not price_series.dropna().empty else 50.0
price_upper = max(50.0, round(price_upper + 1.0, 2))

market_cap_range = _clamp_range_filter_state(
    "screener_market_cap",
    upper_bound=market_cap_upper,
    fallback_upper=50.0,
)
price_range = _clamp_range_filter_state(
    "screener_price",
    upper_bound=price_upper,
    fallback_upper=price_upper,
)

show_favorites_only = bool(st.session_state.get("screener_show_favorites_only", False))
signal_filter = str(st.session_state.get("screener_signal", "Any"))

st.markdown("<div class='screener-section-kicker'>Full Universe</div>", unsafe_allow_html=True)
results_left_col, results_right_col = st.columns([0.76, 0.24], gap="small")
with results_left_col:
    st.subheader("Screening results")
with results_right_col:
    with st.popover("Filters", width="stretch"):
        st.caption("Use the star buttons in the list to pin or unpin favorites.")
        show_favorites_only = st.toggle(
            "Show only favorites",
            key="screener_show_favorites_only",
        )
        if st.button("Reset", key="screener_reset_button", width="stretch"):
            _reset_filters()
            st.rerun()

        market_cap_range = st.slider(
            "Market cap range (USD bn)",
            min_value=0.0,
            max_value=market_cap_upper,
            key="screener_market_cap",
        )
        price_range = st.slider(
            "Stock price range (USD)",
            min_value=0.0,
            max_value=price_upper,
            key="screener_price",
        )
        signal_filter = st.selectbox(
            "Signal contains",
            options=["Any", "Buy", "Hold", "Sell"],
            key="screener_signal",
        )

filters = {
    "market_cap_min": float(market_cap_range[0]),
    "market_cap_max": float(market_cap_range[1]),
    "price_min": float(price_range[0]),
    "price_max": float(price_range[1]),
    "signal": str(signal_filter),
}

filtered_df = _filter_stocks(all_stocks, filters)
filtered_df = filtered_df.sort_values(["market_cap_usd_bn", "ticker"], ascending=[False, True])
if show_favorites_only and favorite_tickers:
    filtered_df = filtered_df.loc[filtered_df["ticker"].isin(favorite_tickers)].copy()

st.caption(f"Showing {len(filtered_df)} of {len(all_stocks)} stocks")

if filtered_df.empty:
    st.info("No stocks match the selected criteria.")
else:
    _render_row_cards(filtered_df, card_prefix="screening", favorite_tickers=favorite_tickers)

#!/usr/bin/env python3
"""Build a Folium map from collected AIS data for the Strait of Hormuz."""

from __future__ import annotations

import argparse
import html
import sqlite3
from pathlib import Path

import folium
import pandas as pd
from folium.plugins import MarkerCluster

DEFAULT_DB_PATH = Path("data/hormuz_ais.sqlite")
DEFAULT_OUTPUT = Path("data/hormuz_map.html")
DEFAULT_CENTER = [26.4, 56.5]
DEFAULT_HOURS = 12


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lag et Folium-kart fra AIS-data")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS)
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--trails", action="store_true")
    return parser.parse_args()


def load_data(db_path: Path, hours: int) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"Fant ikke database: {db_path}")

    sql = """
        SELECT *
        FROM ais_messages
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND received_at_utc >= datetime('now', ?)
        ORDER BY received_at_utc ASC
    """
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=(f"-{int(hours)} hours",))


def keep_latest_per_mmsi(df: pd.DataFrame) -> pd.DataFrame:
    latest = (
        df.sort_values(["mmsi", "received_at_utc"])
        .dropna(subset=["mmsi"])
        .drop_duplicates(subset=["mmsi"], keep="last")
    )
    unknown = df[df["mmsi"].isna()].copy()
    return pd.concat([latest, unknown], ignore_index=True)


def make_popup(row: pd.Series) -> str:
    def safe(value: object) -> str:
        return html.escape("-" if pd.isna(value) else str(value))

    return (
        f"<b>Skip:</b> {safe(row.get('ship_name'))}<br>"
        f"<b>MMSI:</b> {safe(row.get('mmsi'))}<br>"
        f"<b>Melding:</b> {safe(row.get('message_type'))}<br>"
        f"<b>Fart (knop):</b> {safe(row.get('sog'))}<br>"
        f"<b>Kurs:</b> {safe(row.get('cog'))}<br>"
        f"<b>Heading:</b> {safe(row.get('true_heading'))}<br>"
        f"<b>Mottatt:</b> {safe(row.get('received_at_utc'))}<br>"
        f"<b>AIS-tid:</b> {safe(row.get('ais_time_utc'))}"
    )


def build_map(df: pd.DataFrame, output: Path, trails: bool) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    m = folium.Map(location=DEFAULT_CENTER, zoom_start=7, tiles="CartoDB positron")
    cluster = MarkerCluster(name="Siste kjente posisjoner").add_to(m)

    for _, row in df.iterrows():
        tooltip = (row.get("ship_name") or f"MMSI {row.get('mmsi')}") if not pd.isna(row.get("mmsi")) else "Ukjent fartøy"
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=4,
            popup=folium.Popup(make_popup(row), max_width=350),
            tooltip=tooltip,
        ).add_to(cluster)

    if trails:
        for mmsi, group in df.dropna(subset=["mmsi"]).groupby("mmsi"):
            group = group.sort_values("received_at_utc")
            points = group[["latitude", "longitude"]].dropna().values.tolist()
            if len(points) < 2:
                continue
            ship_name = group["ship_name"].dropna()
            label = ship_name.iloc[-1] if not ship_name.empty else f"MMSI {int(mmsi)}"
            folium.PolyLine(points, weight=2, opacity=0.75, tooltip=str(label)).add_to(m)

    folium.LayerControl().add_to(m)
    m.save(str(output))


def main() -> int:
    args = parse_args()
    df = load_data(args.db, args.hours)
    if df.empty:
        raise SystemExit("Ingen posisjonsdata funnet i valgt tidsvindu.")
    if args.latest_only:
        df = keep_latest_per_mmsi(df)
    build_map(df, args.output, trails=args.trails)
    print(f"Kart lagret til {args.output}")
    print(f"Viser {len(df)} posisjoner")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

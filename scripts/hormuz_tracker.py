#!/usr/bin/env python3
"""Build a compact, S3-backed traffic history for crude tankers near Hormuz.

The global oil-flow collector calls :func:`update_hormuz_tracker` after each
TankerMap snapshot.  Only vessels inside a broad Hormuz corridor are retained.
Crossings are inferred when the same IMO moves between the outer west/east
zones.  The deliberately broad zones make the inference useful with a
three-hour sampling interval while a speed sanity check rejects AIS jumps.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
from collections import defaultdict
from zoneinfo import ZoneInfo


HORMUZ_BOUNDS = {
    "south": 23.5,
    "north": 27.8,
    "west": 54.0,
    "east": 60.5,
}
WEST_OUTER_LIMIT = 56.20
EAST_OUTER_LIMIT = 56.90
TRACK_RETENTION_DAYS = 7
STATE_RETENTION_DAYS = 14
MAX_TRANSIT_HOURS = 36
MAX_TRANSIT_SPEED_KNOTS = 35
BARRELS_PER_TONNE = 7.33
CARGO_SHARE_OF_DWT = 0.92
GULF_TIMEZONE = ZoneInfo("Asia/Dubai")


def _parse_time(value: str | dt.datetime) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _missing_key(exc: Exception) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code")
    return code in {"NoSuchKey", "NoSuchBucket", "404"} or exc.__class__.__name__ == "NoSuchKey"


def _read_json(s3, bucket: str, key: str, default):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _missing_key(exc):
            return default
        raise
    return json.loads(obj["Body"].read().decode("utf-8-sig"))


def _write_json(s3, bucket: str, key: str, payload) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache",
    )


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 3440.065 * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))


def _in_hormuz_bounds(vessel: dict) -> bool:
    lat = vessel.get("latitude")
    lon = vessel.get("longitude")
    if lat is None or lon is None:
        return False
    lat, lon = _float(lat, 999), _float(lon, 999)
    return (
        HORMUZ_BOUNDS["south"] <= lat <= HORMUZ_BOUNDS["north"]
        and HORMUZ_BOUNDS["west"] <= lon <= HORMUZ_BOUNDS["east"]
    )


def _outer_side(longitude: float) -> str:
    if longitude <= WEST_OUTER_LIMIT:
        return "west"
    if longitude >= EAST_OUTER_LIMIT:
        return "east"
    return "strait"


def _tanker_class(deadweight: float) -> str:
    if deadweight >= 200_000:
        return "VLCC"
    if deadweight >= 120_000:
        return "Suezmax"
    if deadweight >= 70_000:
        return "Aframax"
    if deadweight >= 45_000:
        return "Panamax"
    return "Other"


def _capacity_barrels(deadweight: float) -> float:
    return deadweight * CARGO_SHARE_OF_DWT * BARRELS_PER_TONNE


def _public_vessel(vessel: dict, collected_at: str) -> dict:
    deadweight = _float(vessel.get("deadweight"))
    cargo_state = str(vessel.get("cargo_state") or "unknown")
    return {
        "imo": str(vessel.get("imo") or ""),
        "name": str(vessel.get("name") or "Unknown"),
        "latitude": _float(vessel.get("latitude")),
        "longitude": _float(vessel.get("longitude")),
        "speed_knots": _float(vessel.get("speed_knots")),
        "deadweight": deadweight,
        "tanker_class": _tanker_class(deadweight),
        "cargo_state": cargo_state,
        "destination": str(vessel.get("destination") or ""),
        "capacity_barrels": round(_capacity_barrels(deadweight)),
        "estimated_cargo_barrels": round(_capacity_barrels(deadweight)) if cargo_state == "loaded" else 0,
        "observed_at": collected_at,
        "zone": _outer_side(_float(vessel.get("longitude"))),
    }


def _movement(previous: dict | None, current: dict) -> str:
    if current["speed_knots"] < 1.0:
        return "stationary"
    if not previous:
        return "unknown"
    longitude_change = current["longitude"] - _float(previous.get("last_lon"))
    if longitude_change > 0.015:
        return "outbound"
    if longitude_change < -0.015:
        return "inbound"
    return "unknown"


def _crossing_event(previous: dict, current: dict, observed_at: dt.datetime) -> dict | None:
    previous_side = previous.get("last_outer_side")
    current_side = current["zone"]
    if previous_side not in {"west", "east"} or current_side not in {"west", "east"}:
        return None
    if previous_side == current_side:
        return None

    try:
        previous_at = _parse_time(previous["last_outer_at"])
        previous_lat = _float(previous["last_outer_lat"])
        previous_lon = _float(previous["last_outer_lon"])
    except (KeyError, TypeError, ValueError):
        return None

    elapsed_hours = (observed_at - previous_at).total_seconds() / 3600
    if elapsed_hours <= 0 or elapsed_hours > MAX_TRANSIT_HOURS:
        return None

    distance_nm = _haversine_nm(
        previous_lat,
        previous_lon,
        current["latitude"],
        current["longitude"],
    )
    if distance_nm / elapsed_hours > MAX_TRANSIT_SPEED_KNOTS:
        return None

    last_crossing_at = previous.get("last_crossing_at")
    if last_crossing_at:
        hours_since_last = (observed_at - _parse_time(last_crossing_at)).total_seconds() / 3600
        if hours_since_last < 18:
            return None

    midpoint = previous_at + (observed_at - previous_at) / 2
    direction = "outbound" if previous_side == "west" else "inbound"
    event_id = f"{current['imo']}-{direction}-{midpoint:%Y%m%dT%H%M}"
    return {
        "event_id": event_id,
        "crossed_at": _iso(midpoint),
        "local_date": midpoint.astimezone(GULF_TIMEZONE).date().isoformat(),
        "imo": current["imo"],
        "name": current["name"],
        "direction": direction,
        "deadweight": round(current["deadweight"]),
        "tanker_class": current["tanker_class"],
        "cargo_state": current["cargo_state"],
        "capacity_barrels": current["capacity_barrels"],
        "estimated_cargo_barrels": current["estimated_cargo_barrels"],
        "destination": current["destination"],
    }


def _daily_rows(crossings: list[dict]) -> list[dict]:
    daily: dict[str, dict] = defaultdict(
        lambda: {
            "date": "",
            "inbound_count": 0,
            "outbound_count": 0,
            "total_count": 0,
            "inbound_dwt": 0,
            "outbound_dwt": 0,
            "inbound_estimated_barrels": 0,
            "outbound_estimated_barrels": 0,
        }
    )
    for event in crossings:
        date = event["local_date"]
        row = daily[date]
        row["date"] = date
        direction = event["direction"]
        row[f"{direction}_count"] += 1
        row["total_count"] += 1
        row[f"{direction}_dwt"] += round(_float(event.get("deadweight")))
        row[f"{direction}_estimated_barrels"] += round(_float(event.get("estimated_cargo_barrels")))
    return [daily[date] for date in sorted(daily)]


def _write_daily_csv(s3, bucket: str, key: str, rows: list[dict]) -> None:
    fields = [
        "date",
        "inbound_count",
        "outbound_count",
        "total_count",
        "inbound_dwt",
        "outbound_dwt",
        "inbound_estimated_barrels",
        "outbound_estimated_barrels",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=output.getvalue().encode("utf-8"),
        ContentType="text/csv",
        CacheControl="no-cache",
    )


def _summary(current: list[dict], crossings: list[dict], daily: list[dict], observed_at: dt.datetime) -> dict:
    cutoff = observed_at - dt.timedelta(hours=24)
    recent = [event for event in crossings if _parse_time(event["crossed_at"]) >= cutoff]
    completed_days = [row for row in daily if row["date"] < observed_at.astimezone(GULF_TIMEZONE).date().isoformat()][-7:]
    seven_day_average = (
        sum(row["total_count"] for row in completed_days) / len(completed_days)
        if completed_days
        else 0
    )
    return {
        "current_vessels": len(current),
        "in_strait": sum(vessel["zone"] == "strait" for vessel in current),
        "inbound_24h": sum(event["direction"] == "inbound" for event in recent),
        "outbound_24h": sum(event["direction"] == "outbound" for event in recent),
        "crossings_24h": len(recent),
        "outbound_cargo_barrels_24h": round(
            sum(_float(event.get("estimated_cargo_barrels")) for event in recent if event["direction"] == "outbound")
        ),
        "seven_day_average": round(seven_day_average, 1),
    }


def update_hormuz_tracker(
    *,
    s3,
    bucket: str,
    prefix: str,
    vessels: list[dict],
    collected_at: str,
) -> dict:
    """Persist one Hormuz observation and return the public dashboard payload."""

    observed_at = _parse_time(collected_at)
    collected_at = _iso(observed_at)
    base_key = f"{prefix.strip('/')}/hormuz"
    state_key = f"{base_key}/state.json"
    crossings_key = f"{base_key}/crossings.json"

    state = _read_json(s3, bucket, state_key, {"version": 1, "vessels": {}, "tracks": {}})
    vessel_states = state.setdefault("vessels", {})
    tracks = state.setdefault("tracks", {})
    crossings = _read_json(s3, bucket, crossings_key, {"crossings": []}).get("crossings", [])
    crossing_ids = {event["event_id"] for event in crossings}
    new_crossings = []
    current = []

    relevant = [vessel for vessel in vessels if vessel.get("imo") and _in_hormuz_bounds(vessel)]
    for raw_vessel in relevant:
        vessel = _public_vessel(raw_vessel, collected_at)
        imo = vessel["imo"]
        previous = vessel_states.get(imo)
        vessel["movement"] = _movement(previous, vessel)

        event = _crossing_event(previous, vessel, observed_at) if previous else None
        if event and event["event_id"] not in crossing_ids:
            crossings.append(event)
            new_crossings.append(event)
            crossing_ids.add(event["event_id"])

        vessel_state = dict(previous or {})
        vessel_state.update(
            {
                "name": vessel["name"],
                "deadweight": vessel["deadweight"],
                "tanker_class": vessel["tanker_class"],
                "cargo_state": vessel["cargo_state"],
                "destination": vessel["destination"],
                "last_seen": collected_at,
                "last_lat": vessel["latitude"],
                "last_lon": vessel["longitude"],
                "last_movement": vessel["movement"],
            }
        )
        if vessel["zone"] in {"west", "east"}:
            vessel_state.update(
                {
                    "last_outer_side": vessel["zone"],
                    "last_outer_at": collected_at,
                    "last_outer_lat": vessel["latitude"],
                    "last_outer_lon": vessel["longitude"],
                }
            )
        if event:
            vessel_state["last_crossing_at"] = event["crossed_at"]
        vessel_states[imo] = vessel_state

        point = {
            "observed_at": collected_at,
            "latitude": vessel["latitude"],
            "longitude": vessel["longitude"],
            "speed_knots": vessel["speed_knots"],
            "movement": vessel["movement"],
        }
        track = tracks.setdefault(imo, [])
        if not track or track[-1]["observed_at"] != collected_at:
            track.append(point)
        current.append(vessel)

    track_cutoff = observed_at - dt.timedelta(days=TRACK_RETENTION_DAYS)
    state_cutoff = observed_at - dt.timedelta(days=STATE_RETENTION_DAYS)
    for imo in list(tracks):
        tracks[imo] = [point for point in tracks[imo] if _parse_time(point["observed_at"]) >= track_cutoff]
        if not tracks[imo]:
            del tracks[imo]
    for imo in list(vessel_states):
        if _parse_time(vessel_states[imo]["last_seen"]) < state_cutoff:
            del vessel_states[imo]

    crossings.sort(key=lambda event: event["crossed_at"])
    daily = _daily_rows(crossings)
    current.sort(key=lambda vessel: vessel["deadweight"], reverse=True)

    snapshot = {"collected_at": collected_at, "vessels": current}
    snapshot_key = (
        f"{base_key}/snapshots/year={observed_at:%Y}/month={observed_at:%m}/day={observed_at:%d}/"
        f"snapshot_{observed_at:%Y%m%dT%H%M%SZ}.json"
    )
    _write_json(s3, bucket, snapshot_key, snapshot)
    _write_json(s3, bucket, crossings_key, {"crossings": crossings})
    _write_json(s3, bucket, state_key, state)
    _write_daily_csv(s3, bucket, f"{base_key}/daily_traffic.csv", daily)

    recent_crossing_cutoff = observed_at - dt.timedelta(days=TRACK_RETENTION_DAYS)
    public_crossings = [
        event for event in crossings if _parse_time(event["crossed_at"]) >= recent_crossing_cutoff
    ]
    public_tracks = {
        imo: points for imo, points in tracks.items() if points
    }
    track_vessels = {
        imo: {
            "imo": imo,
            "name": vessel_states.get(imo, {}).get("name", "Unknown"),
            "deadweight": vessel_states.get(imo, {}).get("deadweight", 0),
            "tanker_class": vessel_states.get(imo, {}).get("tanker_class", "Other"),
            "cargo_state": vessel_states.get(imo, {}).get("cargo_state", "unknown"),
            "destination": vessel_states.get(imo, {}).get("destination", ""),
            "last_seen": vessel_states.get(imo, {}).get("last_seen", points[-1]["observed_at"]),
            "movement": vessel_states.get(imo, {}).get("last_movement", "unknown"),
        }
        for imo, points in public_tracks.items()
    }
    payload = {
        "ok": True,
        "collected_at": collected_at,
        "source": "TankerMap AIS snapshots",
        "bounds": HORMUZ_BOUNDS,
        "zone_limits": {"west": WEST_OUTER_LIMIT, "east": EAST_OUTER_LIMIT},
        "summary": _summary(current, crossings, daily, observed_at),
        "current_vessels": current,
        "tracks": public_tracks,
        "track_vessels": track_vessels,
        "crossings": public_crossings,
        "daily": daily[-180:],
        "meta": {
            "new_crossings": len(new_crossings),
            "track_retention_days": TRACK_RETENTION_DAYS,
            "sampling": "Approximately every three hours",
            "method": "Unique IMO transitions between broad west/east Hormuz zones",
            "timezone": "Asia/Dubai",
        },
    }
    _write_json(s3, bucket, f"{base_key}/latest.json", payload)
    return payload

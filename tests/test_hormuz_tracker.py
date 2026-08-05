import io
import json

from scripts.hormuz_tracker import update_hormuz_tracker


class MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, *, Bucket, Key):
        try:
            body = self.objects[(Bucket, Key)]["Body"]
        except KeyError as exc:
            raise MissingKey() from exc
        return {"Body": io.BytesIO(body)}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = {"Body": Body, **kwargs}


def vessel(imo, longitude, *, latitude=26.2, loaded=True, deadweight=300_000):
    return {
        "imo": str(imo),
        "name": f"Tanker {imo}",
        "latitude": latitude,
        "longitude": longitude,
        "speed_knots": 12,
        "deadweight": deadweight,
        "cargo_state": "loaded" if loaded else "ballast",
        "destination": "FUJAIRAH",
    }


def run(s3, timestamp, vessels):
    return update_hormuz_tracker(
        s3=s3,
        bucket="test-bucket",
        prefix="oil-tanker-data",
        vessels=vessels,
        collected_at=timestamp,
    )


def test_inbound_crossing_survives_middle_zone_and_is_counted_once():
    s3 = FakeS3()
    first = run(s3, "2026-07-15T00:00:00+00:00", [vessel(1001, 57.2)])
    assert first["summary"]["crossings_24h"] == 0

    middle = run(s3, "2026-07-15T03:00:00+00:00", [vessel(1001, 56.55)])
    assert middle["summary"]["crossings_24h"] == 0

    crossed = run(s3, "2026-07-15T06:00:00+00:00", [vessel(1001, 56.0)])
    assert crossed["summary"]["inbound_24h"] == 1
    assert crossed["summary"]["outbound_24h"] == 0
    assert crossed["daily"][-1]["inbound_count"] == 1

    repeated = run(s3, "2026-07-15T09:00:00+00:00", [vessel(1001, 55.8)])
    assert repeated["summary"]["inbound_24h"] == 1
    assert len(repeated["crossings"]) == 1


def test_loaded_outbound_crossing_adds_estimated_volume():
    s3 = FakeS3()
    run(s3, "2026-07-15T00:00:00+00:00", [vessel(2002, 55.8, deadweight=280_000)])
    payload = run(s3, "2026-07-15T06:00:00+00:00", [vessel(2002, 57.1, deadweight=280_000)])

    event = payload["crossings"][0]
    assert event["direction"] == "outbound"
    assert event["estimated_cargo_barrels"] > 1_800_000
    assert payload["summary"]["outbound_cargo_barrels_24h"] == event["estimated_cargo_barrels"]
    assert payload["daily"][-1]["outbound_count"] == 1


def test_outside_bounds_and_implausible_gap_do_not_create_crossings():
    s3 = FakeS3()
    first = run(
        s3,
        "2026-07-15T00:00:00+00:00",
        [vessel(3003, 61.0), vessel(4004, 57.2)],
    )
    assert [item["imo"] for item in first["current_vessels"]] == ["4004"]

    payload = run(s3, "2026-07-17T00:00:00+00:00", [vessel(4004, 55.8)])
    assert payload["summary"]["crossings_24h"] == 0


def test_track_points_are_retained_for_thirty_days():
    s3 = FakeS3()
    run(s3, "2026-07-01T00:00:00+00:00", [vessel(6006, 57.2)])

    # 20 days on: the original point is well past the old 7-day window but must survive.
    twenty = run(s3, "2026-07-21T00:00:00+00:00", [vessel(6006, 57.2)])
    assert len(twenty["tracks"]["6006"]) == 2

    # 40 days after the first fix: only the point older than 30 days is pruned.
    forty = run(s3, "2026-08-10T00:00:00+00:00", [vessel(6006, 57.2)])
    observed = [point["observed_at"] for point in forty["tracks"]["6006"]]
    assert observed == ["2026-07-21T00:00:00+00:00", "2026-08-10T00:00:00+00:00"]


def test_public_payload_and_csv_are_written():
    s3 = FakeS3()
    payload = run(s3, "2026-07-15T00:00:00+00:00", [vessel(5005, 57.2)])

    latest = json.loads(
        s3.objects[("test-bucket", "oil-tanker-data/hormuz/latest.json")]["Body"].decode("utf-8")
    )
    assert latest["collected_at"] == payload["collected_at"]
    assert latest["track_vessels"]["5005"]["name"] == "Tanker 5005"
    assert ("test-bucket", "oil-tanker-data/hormuz/daily_traffic.csv") in s3.objects


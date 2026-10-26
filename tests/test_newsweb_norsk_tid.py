"""NewsWeb-siden viser tidspunkter i norsk tid."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import newsweb_routes as nr  # noqa: E402


def test_norsk_tid_vinter_og_sommer():
    assert nr.norsk_tid("2026-10-26T08:12:00Z") == "2026-10-26 09:12"          # vintertid, +1
    assert nr.norsk_tid("2026-10-26T07:43:26+00:00") == "2026-10-26 08:43"
    assert nr.norsk_tid("2026-07-01T10:00:00") == "2026-07-01 12:00"           # uten sone = UTC, sommertid +2


def test_norsk_tid_ukjent_og_tom():
    assert nr.norsk_tid("ikke en dato") == "ikke en dato"
    assert nr.norsk_tid(None) == ""

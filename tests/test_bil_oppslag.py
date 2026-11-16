import pytest

from bil_routes import _parse_bil_oppslag_input


@pytest.mark.parametrize(
    ("raw", "expected_type", "expected_value"),
    [
        ("471343742", "finnkode", "471343742"),
        (
            "https://www.finn.no/mobility/item/471343742",
            "finnkode",
            "471343742",
        ),
        (
            "https://www.finn.no/car/used/ad.html?finnkode=471343742",
            "finnkode",
            "471343742",
        ),
        ("EE 24350", "regnr", "EE24350"),
        ("ab-12345", "regnr", "AB12345"),
        ("YS3FD49Y361123456", "vin", "YS3FD49Y361123456"),
        ("12345678901234567", "vin", "12345678901234567"),
        ("ABC12345678901", "vin", "ABC12345678901"),
        ("RN", "regnr", "RN"),
    ],
)
def test_parse_bil_oppslag_input(raw, expected_type, expected_value):
    search_type, value, error = _parse_bil_oppslag_input(raw)

    assert error is None
    assert search_type == expected_type
    assert value == expected_value


@pytest.mark.parametrize("raw", ["", "  ", "---", "A"])
def test_parse_bil_oppslag_input_rejects_invalid_values(raw):
    search_type, value, error = _parse_bil_oppslag_input(raw)

    assert search_type is None
    assert value is None
    assert error


@pytest.fixture
def lookup_client(tmp_path, monkeypatch):
    import duckdb
    from flask import Flask
    import bil_routes

    con = duckdb.connect()
    con.execute('''CREATE TABLE cars AS
        SELECT CAST(480000000 + i AS VARCHAR) AS FinnKode,
               'EK 87202' AS regnr, 'SJNFAAZE1U0010837' AS svv_vin,
               '2026-01-01' AS Dato,
               CAST(DATE '2026-01-01' + CAST(i AS INTEGER) AS VARCHAR) AS Dato_ny,
               100000 + i AS Pris_ny, 'NEI' AS Solgt, '' AS Selger
        FROM range(25) t(i)''')
    # A changed plate is still the same car through VIN; an unrelated car is excluded.
    con.execute("UPDATE cars SET regnr = 'AB12345' WHERE FinnKode = '480000000'")
    con.execute("INSERT INTO cars VALUES ('999999999', 'ZZ99999', NULL, NULL, NULL, 1, 'NEI', '')")
    path = str(tmp_path / 'cars.parquet')
    con.execute("COPY cars TO ? (FORMAT PARQUET)", [path])
    monkeypatch.setattr(bil_routes, '_ensure_local_parquet', lambda key: path)
    monkeypatch.setattr(bil_routes, '_duckdb_con', lambda: con)
    monkeypatch.setattr(bil_routes, '_ensure_active_finnkoder_table', lambda: False)
    monkeypatch.setattr(bil_routes, '_PARQUET_CACHE', {bil_routes.PARQUET_KEY_SOLGT: {}})
    app = Flask(__name__)
    app.register_blueprint(bil_routes.bil_bp)
    yield app.test_client()
    con.close()


@pytest.mark.parametrize('identifier', ['EK87202', 'SJNFAAZE1U0010837', '480000010'])
def test_lookup_returns_full_vehicle_history(lookup_client, identifier):
    response = lookup_client.post('/bil/oppslag/data', json={'identifier': identifier})
    assert response.status_code == 200
    data = response.get_json()
    assert data['antall_treff'] == 25
    matches = data['matchende_annonser']
    assert matches[0]['finnkode'] == '480000024'
    assert matches[-1]['finnkode'] == '480000000'
    assert matches[0]['pris'] == 100024
    assert data['finnkode'] == ('480000010' if identifier.isdigit() else '480000024')
    assert data['alle_felt']['FinnKode'] == data['finnkode']


def test_lookup_without_vehicle_identifiers_still_works(lookup_client):
    data = lookup_client.post('/bil/oppslag/data', json={'finnkode': '999999999'}).get_json()
    assert data['antall_treff'] == 1
    assert data['finnkode'] == '999999999'


def test_lookup_unknown_vehicle(lookup_client):
    data = lookup_client.post('/bil/oppslag/data', json={'identifier': 'XX12345'}).get_json()
    assert data['funnet'] is False

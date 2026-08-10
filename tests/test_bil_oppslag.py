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

"""Basic target smoke tests."""

from __future__ import annotations

from target_apteco.sinks import ContactsSink, TransactionsSink
from target_apteco.target import TargetApteco


def test_target_metadata():
    assert TargetApteco.name == "target-apteco"
    assert ContactsSink in TargetApteco.SINK_TYPES
    assert TransactionsSink in TargetApteco.SINK_TYPES


def test_contacts_mapping_blackbaud_shape():
    sink = ContactsSink.__new__(ContactsSink)
    mapped = ContactsSink.map_record(
        sink,
        {
            "id": "123",
            "first": "Alice",
            "last": "Donor",
            "email": {"address": "alice@example.com"},
            "phone": {"number": "+15551212", "type": "Mobile"},
            "address": {
                "address_lines": "1 Main St",
                "city": "Boston",
                "postal_code": "02108",
                "country": "US",
            },
            "birthdate": {"y": 1985, "m": 4, "d": 12},
        },
    )
    assert mapped["Source Unique Reference"] == "123"
    assert mapped["FirstName"] == "Alice"
    assert mapped["LastName"] == "Donor"
    assert mapped["Primary Email Address"] == "alice@example.com"
    assert mapped["Primary Mobile Phone Number"] == "+15551212"
    assert mapped["Date Of Birth"] == "1985-04-12"


def test_transactions_mapping_gift_shape():
    sink = TransactionsSink.__new__(TransactionsSink)
    mapped = TransactionsSink.map_record(
        sink,
        {
            "id": "gift-9",
            "constituent_id": "123",
            "amount": {"value": 50.0},
            "date": "2024-01-15",
            "type": "Donation",
        },
    )
    assert mapped["Source Unique Reference"] == "123"
    assert mapped["Transaction Source URN"] == "gift-9"
    assert mapped["Amount"] == 50.0
    assert mapped["Transaction Date"] == "2024-01-15"

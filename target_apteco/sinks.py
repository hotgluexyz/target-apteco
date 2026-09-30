"""Apteco Contacts and Transactions sinks."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests
import singer

from target_apteco.client import AptecoSink

LOGGER = singer.get_logger()


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _nested(value: Any, *keys: str) -> Any:
    if not isinstance(value, dict):
        return None
    for key in keys:
        if value.get(key) is not None and value.get(key) != "":
            return value[key]
    return None


def _format_birthdate(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        year = value.get("y") or value.get("year")
        month = value.get("m") or value.get("month")
        day = value.get("d") or value.get("day")
        if year and month and day:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return None
    return str(value)[:10]


def _address_line1(address: Any) -> Optional[str]:
    if isinstance(address, dict):
        lines = address.get("address_lines") or address.get("line1")
        if isinstance(lines, str):
            return lines.split("\n")[0]
        return lines
    if isinstance(address, list) and address:
        return _address_line1(address[0])
    return None


class ContactsSink(AptecoSink):
    """Map Blackbaud constituents / unified Contacts into Apteco Individuals."""

    name = "Contacts"
    meta_table_name = "Individuals"
    data_source_title = "hotglue-Contacts"
    dedupe_rule_codes = ["SourceURN"]

    # Segmentation columns produced by etl-scripts/etl.py for Apteco CDP attributes.
    SEGMENTATION_FIELDS = (
        "seg_donor_tier",
        "seg_interest",
        "seg_volunteer",
        "tags",
    )

    def csv_fieldnames(self) -> List[str]:
        return [
            "Source Unique Reference",
            "Title",
            "FirstName",
            "MiddleName",
            "LastName",
            "Gender",
            "Date Of Birth",
            "Primary Email Address",
            "Primary Mobile Phone Number",
            "Primary Landline Phone Number",
            "Primary Address Line 1",
            "Primary Address Line 2",
            "Primary Address Town",
            "Primary Address County",
            "Primary Address Postcode",
            "Primary Address Country",
            "Primary Address Country Code",
            "Individual Source Create Date",
            "Individual Source Update Date",
            "seg_donor_tier",
            "seg_interest",
            "seg_volunteer",
            "tags",
        ]

    def map_record(self, record: dict) -> dict:
        email = _first(
            record.get("email") if not isinstance(record.get("email"), dict) else None,
            _nested(record.get("email"), "address", "email"),
        )
        phone = _first(
            record.get("phone") if not isinstance(record.get("phone"), dict) else None,
            _nested(record.get("phone"), "number", "phone"),
            record.get("mobile_phone"),
        )
        address = record.get("address")
        if isinstance(address, list) and address:
            address = address[0]
        if not isinstance(address, dict):
            address = {}

        phone_type = (_nested(record.get("phone"), "type") or "").lower()
        if "land" in phone_type or "home" in phone_type or "work" in phone_type:
            mobile, landline = None, phone
        else:
            mobile, landline = phone, None

        mapped = {
            "Source Unique Reference": str(
                _first(record.get("id"), record.get("lookup_id"), record.get("externalId"), record.get("external_id"))
                or ""
            ),
            "Title": _first(record.get("title"), record.get("salutation")),
            "FirstName": _first(record.get("first_name"), record.get("first")),
            "MiddleName": _first(record.get("middle_name"), record.get("middle")),
            "LastName": _first(record.get("last_name"), record.get("last")),
            "Gender": record.get("gender"),
            "Date Of Birth": _format_birthdate(
                _first(record.get("birthdate"), record.get("date_of_birth"), record.get("dob"))
            ),
            "Primary Email Address": email,
            "Primary Mobile Phone Number": mobile,
            "Primary Landline Phone Number": landline if landline != mobile else None,
            "Primary Address Line 1": _first(
                _address_line1(address),
                address.get("line1"),
                record.get("address_line_1"),
            ),
            "Primary Address Line 2": _first(address.get("line2"), record.get("address_line_2")),
            "Primary Address Town": _first(address.get("city"), record.get("city")),
            "Primary Address County": _first(address.get("county"), address.get("state"), record.get("state")),
            "Primary Address Postcode": _first(
                address.get("postal_code"), address.get("zip"), record.get("postal_code")
            ),
            "Primary Address Country": _first(address.get("country"), record.get("country")),
            "Primary Address Country Code": _first(
                address.get("country_code"),
                address.get("country"),
                record.get("country_code"),
            ),
            "Individual Source Create Date": _first(record.get("date_added"), record.get("created_at")),
            "Individual Source Update Date": _first(record.get("date_modified"), record.get("updated_at")),
        }
        for field in self.SEGMENTATION_FIELDS:
            mapped[field] = record.get(field)
        return mapped

    def build_table_mapping_columns(self, source_table_id: int) -> List[dict]:
        """Map core Individuals columns, then attach seg_* as CDP text attributes."""
        columns = super().build_table_mapping_columns(source_table_id)
        mapped_eav_ids = {col.get("eavAttributeId") for col in columns if col.get("eavAttributeId")}
        eav_attributes = self.get_eav_attributes(source_table_id)
        for attr in eav_attributes:
            name = attr.get("name")
            eav_id = attr.get("id")
            if name not in self.SEGMENTATION_FIELDS or eav_id in mapped_eav_ids:
                continue
            attribute_id = self.ensure_individual_text_attribute(name)
            if attribute_id is None:
                continue
            columns.append({"eavAttributeId": eav_id, "attributeId": attribute_id})
            mapped_eav_ids.add(eav_id)
            LOGGER.info("Mapped segmentation column %s -> attribute %s", name, attribute_id)
        return columns

    def ensure_individual_text_attribute(self, description: str) -> Optional[int]:
        """Create or reuse a selectable text CDP attribute for Individuals."""
        try:
            response = requests.get(
                self.url("CDP/Attributes"),
                headers=self.authenticator.auth_headers,
                timeout=60,
            )
            if response.ok:
                for item in response.json() or []:
                    if item.get("description") == description and not item.get("deletionDate"):
                        return int(item["attributeId"])

            metas = self.request_api("GET", endpoint="CDP/MetaTables").json() or []
            meta_table_id = next(
                (
                    table["metaTableId"]
                    for table in metas
                    if table.get("tableName") in (
                        "Individual Attributes",
                        "Individuals",
                    )
                ),
                None,
            )
            if meta_table_id is None:
                # Common Apteco CDP id for individual attributes; best-effort fallback.
                meta_table_id = 3

            response = requests.post(
                self.url("CDP/Attributes"),
                headers={**self.authenticator.auth_headers, "Content-Type": "application/json"},
                json={
                    "description": description,
                    "variableType": "Selector",
                    "metaTableId": meta_table_id,
                    "addCodeToDescriptions": False,
                    "addUnclassified": False,
                    "selectable": True,
                    "browseable": True,
                    "exportable": True,
                    "exportDescriptions": True,
                    "included": True,
                },
                timeout=60,
            )
            if not response.ok:
                LOGGER.warning(
                    "Could not create individual attribute %s (%s): %s",
                    description,
                    response.status_code,
                    response.text[:500],
                )
                return None
            return int(response.json()["attributeId"])
        except Exception as exc:
            LOGGER.warning("Could not create individual attribute %s: %s", description, exc)
            return None


class ConstituentsSink(ContactsSink):
    """Alias for Blackbaud `constituents` stream name."""

    name = "constituents"


class TransactionsSink(AptecoSink):
    """Map gifts / donations into Apteco Individual Transactions."""

    name = "Transactions"
    meta_table_name = "Individual Transactions"
    data_source_title = "hotglue-Transactions"
    dedupe_rule_codes = ["SourceURN"]
    transaction_type_name = "Gift"
    amount_attribute_name = "Amount"

    def csv_fieldnames(self) -> List[str]:
        return [
            "Source Unique Reference",
            "Transaction Source URN",
            "Transaction Date",
            "Transaction Type",
            "Amount",
            "Source Create Date",
            "Source Update Date",
        ]

    def map_record(self, record: dict) -> dict:
        raw_amount = record.get("amount")
        amount = _first(
            _nested(raw_amount, "value") if isinstance(raw_amount, dict) else raw_amount,
            record.get("gift_amount"),
        )
        return {
            "Source Unique Reference": str(
                _first(
                    record.get("contactExternalId"),
                    record.get("contact_external_id"),
                    record.get("constituent_id"),
                    record.get("contact_id"),
                    record.get("customer_id"),
                    _nested(record.get("constituent"), "id"),
                    _nested(record.get("contact"), "id"),
                )
                or ""
            ),
            "Transaction Source URN": str(
                _first(record.get("id"), record.get("gift_id"), record.get("externalId"), record.get("external_id"))
                or ""
            ),
            "Transaction Date": _first(
                record.get("date"),
                record.get("gift_date"),
                record.get("transaction_date"),
                record.get("posted_date"),
            ),
            "Transaction Type": _first(
                record.get("transaction_type"),
                record.get("type"),
                record.get("gift_type"),
                self.transaction_type_name,
            ),
            "Amount": amount,
            "Source Create Date": _first(record.get("date_added"), record.get("created_at")),
            "Source Update Date": _first(record.get("date_modified"), record.get("updated_at")),
        }


class GiftsSink(TransactionsSink):
    """Alias for Blackbaud `gifts` stream name."""

    name = "gifts"

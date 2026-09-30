"""Apteco target class."""

from __future__ import annotations

from typing import Type

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.sinks import Sink
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_apteco.sinks import (
    ConstituentsSink,
    ContactsSink,
    GiftsSink,
    TransactionsSink,
)


class TargetApteco(TargetHotglue):
    """Singer target that loads Contacts and Transactions into Apteco CDP."""

    name = "target-apteco"
    SINK_TYPES = [ContactsSink, ConstituentsSink, TransactionsSink, GiftsSink]
    MAX_PARALLELISM = 1
    alerting_level = AlertingLevel.ERROR

    config_jsonschema = th.PropertiesList(
        th.Property("username", th.StringType, required=True, description="Apteco Orbit username / email"),
        th.Property("password", th.StringType, required=True, description="Apteco Orbit password"),
        th.Property(
            "base_url",
            th.StringType,
            default="https://hotglue.ca-1.apteco.cloud",
            description="Apteco cloud host, e.g. https://hotglue.ca-1.apteco.cloud",
        ),
        th.Property(
            "data_view_name",
            th.StringType,
            default="DB01",
            description="Orbit DataView name",
        ),
        th.Property(
            "cdp_source_name",
            th.StringType,
            default="Blackbaud Raiser's Edge",
            description="CDP source label used for identity resolution",
        ),
        th.Property("orbit_api_url", th.StringType, description="Override OrbitAPI base URL"),
        th.Property("connect_api_url", th.StringType, description="Override OrbitConnectAPI base URL"),
        th.Property("contacts_data_source_title", th.StringType),
        th.Property("transactions_data_source_title", th.StringType),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        aliases = {
            "contacts": ContactsSink,
            "constituents": ConstituentsSink,
            "constituents_by_list": ConstituentsSink,
            "transactions": TransactionsSink,
            "gifts": GiftsSink,
            "donations": TransactionsSink,
        }
        return aliases.get(stream_name.lower()) or super().get_sink_class(stream_name)


if __name__ == "__main__":
    TargetApteco.cli()

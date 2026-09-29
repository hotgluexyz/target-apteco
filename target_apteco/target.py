"""apteco target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue

from target_apteco.sinks import (
    aptecoSink,
)


class Targetapteco(Target, TargetHotglue):
    """Sample target for apteco."""

    name = "target-apteco"

    SINK_TYPES = []
    MAX_PARALLELISM = 1

    def get_sink_class(self, stream_name: str):
        return aptecoSink


if __name__ == "__main__":
    Targetapteco.cli()

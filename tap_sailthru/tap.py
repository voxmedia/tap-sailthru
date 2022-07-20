"""sailthru tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_sailthru.streams import (
    AccountsStream,
    BlastStream,
    BlastStatsStream,
    BlastQueryStream,
    ListStream,
    ListStatsStream,
    ListMemberStream,
    UsersStream
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    AccountsStream,
    BlastStream,
    BlastStatsStream,
    BlastQueryStream,
    ListStream,
    ListStatsStream,
    ListMemberStream,
    UsersStream
]


class Tapsailthru(Tap):
    """sailthru tap class."""
    name = "tap-sailthru"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "account_name",
            th.StringType,
            required=True,
            description="Name of the sailthru account"
        ),
        th.Property(
            "api_key",
            th.StringType(),
            required=True,
            description="API key for the sailthru account"
        ),
        th.Property(
            "api_secret",
            th.StringType(),
            required=True,
            description="API secret for the sailthru account"
        ),
        th.Property(
            "user_agent",
            th.StringType,
            required=True,
            description="Project IDs to replicate"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=300,
            description="The url for the API service"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

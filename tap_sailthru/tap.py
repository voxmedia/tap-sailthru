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
            "accounts",
            th.ArrayType(th.ObjectType(
                th.Property("api_key", th.StringType()),
                th.Property("api_secret", th.StringType()),
                th.Property("name", th.StringType()),
            )),
            required=True,
            description="The accounts"
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

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync(context=self._config)
            stream.finalize_state_progress_markers()

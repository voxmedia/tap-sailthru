"""Tests standard tap features using the built-in SDK tests library."""

import os

from singer_sdk.helpers._util import read_json_file
from singer_sdk.testing import get_standard_tap_tests

from tap_sailthru.tap import Tapsailthru

CONFIG_PATH = ".secrets/config.json"

if os.getenv("CI"):  # true when running a GitHub Actions workflow
    SAMPLE_CONFIG = {
        "api_key": os.getenv("TAP_SAILTHRU_API_KEY"),
        "api_secret": os.getenv("TAP_SAILTHRU_API_KEY"),
        "account_name": "nymag",
    }
else:
    SAMPLE_CONFIG = read_json_file(CONFIG_PATH)


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(Tapsailthru, config=SAMPLE_CONFIG)
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.

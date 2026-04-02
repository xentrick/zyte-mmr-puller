import asyncio
import base64
import json
import unittest
from typing import Any, cast

from zyte.core import ZyteMMRPuller


def _encode_body(payload: dict) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")


def _profile_payload() -> dict:
    return {
        "data": {
            "availableSegments": [],
            "expiryDate": "2026-04-02T00:00:00+00:00",
            "metadata": {
                "currentSeason": 1,
                "lastUpdated": {
                    "displayValue": "2026-04-02T00:00:00+00:00",
                    "value": "2026-04-02T00:00:00+00:00",
                },
                "playerId": 123,
            },
            "platformInfo": {
                "platformSlug": "steam",
                "platformUserHandle": "example",
                "platformUserId": "1",
                "platformUserIdentifier": "example",
            },
            "segments": [],
        }
    }


def _season_payload() -> dict:
    return {
        "data": [
            {
                "type": "playlist",
                "attributes": {"playlistId": 11, "season": 1},
                "metadata": {"name": "Ranked Doubles 2v2"},
                "expiryDate": "0001-01-01T00:00:00+00:00",
                "stats": {},
            }
        ]
    }


class FakeSyncClient:
    def __init__(self, response: dict):
        self.response = response
        self.last_opts: dict | None = None

    def get(self, opts: dict) -> dict:
        self.last_opts = opts
        return self.response


class FakeAsyncClient:
    def __init__(self, response: dict):
        self.response = response
        self.last_opts: dict | None = None

    async def get(self, opts: dict) -> dict:
        self.last_opts = opts
        return self.response


class TestCoreSyncAsync(unittest.TestCase):
    def test_pull_mmr_sync(self) -> None:
        puller = ZyteMMRPuller(api_key="test")
        puller.client = cast(
            Any,
            FakeSyncClient({"httpResponseBody": _encode_body(_profile_payload())}),
        )

        profile = puller.pull_mmr(
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        )

        self.assertIsNotNone(profile)
        if profile is None:
            self.fail("Expected profile payload")
        self.assertEqual(profile.data.metadata.playerId, 123)

    def test_pull_season_sync(self) -> None:
        puller = ZyteMMRPuller(api_key="test")
        puller.client = cast(
            Any,
            FakeSyncClient({"httpResponseBody": _encode_body(_season_payload())}),
        )

        payload = puller.pull_season(
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
            season=1,
        )

        self.assertIsNotNone(payload)
        if payload is None:
            self.fail("Expected season payload")
        self.assertEqual(len(payload.data), 1)
        self.assertEqual(payload.data[0].attributes.playlistId, 11)

    def test_pull_mmr_async(self) -> None:
        puller = ZyteMMRPuller(api_key="test")
        puller.async_client = cast(
            Any,
            FakeAsyncClient({"httpResponseBody": _encode_body(_profile_payload())}),
        )

        profile = asyncio.run(
            puller.pull_mmr_async(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
            )
        )

        self.assertIsNotNone(profile)
        if profile is None:
            self.fail("Expected profile payload")
        self.assertEqual(profile.data.metadata.playerId, 123)

    def test_pull_season_async(self) -> None:
        puller = ZyteMMRPuller(api_key="test")
        puller.async_client = cast(
            Any,
            FakeAsyncClient({"httpResponseBody": _encode_body(_season_payload())}),
        )

        payload = asyncio.run(
            puller.pull_season_async(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                season=1,
            )
        )

        self.assertIsNotNone(payload)
        if payload is None:
            self.fail("Expected season payload")
        self.assertEqual(len(payload.data), 1)
        self.assertEqual(payload.data[0].attributes.playlistId, 11)


if __name__ == "__main__":
    unittest.main()

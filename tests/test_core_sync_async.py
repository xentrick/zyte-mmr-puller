import asyncio
import base64
import json
import unittest
from typing import Any, cast
from unittest.mock import patch

from zyte.core import ZyteMMRPuller
from zyte.models import CurrentMMRResult


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
    @patch("zyte.core.AsyncZyteAPI")
    @patch("zyte.core.ZyteAPI")
    def test_init_uses_tracker_gg_aggressive_retry_policy(
        self,
        mock_sync_client_cls,
        mock_async_client_cls,
    ) -> None:
        ZyteMMRPuller(api_key="test")

        self.assertEqual(mock_sync_client_cls.call_count, 1)
        self.assertEqual(mock_async_client_cls.call_count, 1)

        sync_retrying = mock_sync_client_cls.call_args.kwargs.get("retrying")
        async_retrying = mock_async_client_cls.call_args.kwargs.get("retrying")

        self.assertIsNotNone(sync_retrying)
        self.assertIsNotNone(async_retrying)
        self.assertIs(sync_retrying, async_retrying)

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

    def test_get_peak_mmr_by_recent_seasons_uses_five_season_window(self) -> None:
        puller = ZyteMMRPuller(api_key="test")

        season_payload = {
            36: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 11, "season": 36},
                        "metadata": {"name": "Ranked Doubles 2v2"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 1490, "displayValue": "1,490"},
                            "matchesPlayed": {"value": 20, "displayValue": "20"},
                        },
                    }
                ]
            },
            35: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 11, "season": 35},
                        "metadata": {"name": "Ranked Doubles 2v2"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 1510, "displayValue": "1,510"},
                            "matchesPlayed": {"value": 15, "displayValue": "15"},
                        },
                    }
                ]
            },
            34: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 11, "season": 34},
                        "metadata": {"name": "Ranked Doubles 2v2"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 1505, "displayValue": "1,505"},
                            "matchesPlayed": {"value": 10, "displayValue": "10"},
                        },
                    }
                ]
            },
            33: {"data": []},
            32: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 13, "season": 32},
                        "metadata": {"name": "Ranked Standard 3v3"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 1400, "displayValue": "1,400"},
                            "matchesPlayed": {"value": 40, "displayValue": "40"},
                        },
                    }
                ]
            },
        }

        pulled_seasons: list[int] = []

        def fake_pull(
            self: ZyteMMRPuller,
            url: str,
            season: int,
            http_response_body: bool = True,
        ):
            del self, url, http_response_body
            pulled_seasons.append(season)
            payload = season_payload.get(season)
            if payload is None:
                return None
            return puller._validate_segment_playlist(payload)

        with patch.object(ZyteMMRPuller, "pull_season", autospec=True) as mock_pull:
            mock_pull.side_effect = fake_pull

            peaks = puller.get_peak_mmr_by_recent_seasons(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                start_season=36,
            )

        self.assertEqual(pulled_seasons, [36, 35, 34, 33, 32])
        self.assertEqual(len(peaks.playlists), 2)

        doubles = next(item for item in peaks.playlists if item.playlist_id == 11)
        self.assertEqual(doubles.season, 35)
        self.assertEqual(doubles.rank_rating, 1510)

        threes = next(item for item in peaks.playlists if item.playlist_id == 13)
        self.assertEqual(threes.season, 32)
        self.assertEqual(threes.rank_rating, 1400)

    def test_get_peak_mmr_by_recent_seasons_validates_inputs(self) -> None:
        puller = ZyteMMRPuller(api_key="test")

        with self.assertRaises(ValueError):
            puller.get_peak_mmr_by_recent_seasons(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                start_season=0,
            )

        with self.assertRaises(ValueError):
            puller.get_peak_mmr_by_recent_seasons(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                start_season=36,
                seasons_to_scan=0,
            )

    def test_get_peak_mmr_by_recent_seasons_async_uses_five_season_window(self) -> None:
        puller = ZyteMMRPuller(api_key="test")

        season_payload = {
            36: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 10, "season": 36},
                        "metadata": {"name": "Ranked Duel 1v1"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 950, "displayValue": "950"},
                            "matchesPlayed": {"value": 7, "displayValue": "7"},
                        },
                    }
                ]
            },
            35: {
                "data": [
                    {
                        "type": "playlist",
                        "attributes": {"playlistId": 10, "season": 35},
                        "metadata": {"name": "Ranked Duel 1v1"},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "stats": {
                            "peakRating": {"value": 980, "displayValue": "980"},
                            "matchesPlayed": {"value": 10, "displayValue": "10"},
                        },
                    }
                ]
            },
            34: {"data": []},
            33: {"data": []},
            32: {"data": []},
        }

        pulled_seasons: list[int] = []

        async def fake_pull_async(
            self: ZyteMMRPuller,
            url: str,
            season: int,
            http_response_body: bool = True,
        ):
            del self, url, http_response_body
            pulled_seasons.append(season)
            payload = season_payload.get(season)
            if payload is None:
                return None
            return puller._validate_segment_playlist(payload)

        with patch.object(
            ZyteMMRPuller,
            "pull_season_async",
            autospec=True,
        ) as mock_pull_async:
            mock_pull_async.side_effect = fake_pull_async

            peaks = asyncio.run(
                puller.get_peak_mmr_by_recent_seasons_async(
                    "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                    start_season=36,
                )
            )

        self.assertEqual(pulled_seasons, [36, 35, 34, 33, 32])
        self.assertEqual(len(peaks.playlists), 1)
        self.assertEqual(peaks.playlists[0].playlist_id, 10)
        self.assertEqual(peaks.playlists[0].season, 35)
        self.assertEqual(peaks.playlists[0].rank_rating, 980)

    def test_get_current_mmr_by_window_uses_first_season_with_data(self) -> None:
        puller = ZyteMMRPuller(api_key="test")

        empty = CurrentMMRResult(season=36, playlists=[])
        resolved = CurrentMMRResult(
            season=35,
            playlists=[
                {
                    "playlist_id": 11,
                    "playlist_name": "Ranked Doubles 2v2",
                    "games_played": 12,
                    "rank_rating": 1501,
                }
            ],
        )

        with patch.object(
            puller,
            "get_current_mmr_by_season",
            side_effect=[empty, resolved],
        ) as mock_get:
            current = puller.get_current_mmr_by_window(
                "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                start_season=36,
                seasons_to_scan=5,
            )

        self.assertEqual(current.season, 35)
        self.assertEqual(len(current.playlists), 1)
        self.assertEqual(mock_get.call_count, 2)

    def test_get_current_mmr_by_window_async_uses_first_season_with_data(self) -> None:
        puller = ZyteMMRPuller(api_key="test")

        empty = CurrentMMRResult(season=36, playlists=[])
        resolved = CurrentMMRResult(
            season=34,
            playlists=[
                {
                    "playlist_id": 13,
                    "playlist_name": "Ranked Standard 3v3",
                    "games_played": 7,
                    "rank_rating": 1410,
                }
            ],
        )

        with patch.object(
            puller,
            "get_current_mmr_by_season_async",
            side_effect=[empty, empty, resolved],
        ) as mock_get:
            current = asyncio.run(
                puller.get_current_mmr_by_window_async(
                    "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1",
                    start_season=36,
                    seasons_to_scan=5,
                )
            )

        self.assertEqual(current.season, 34)
        self.assertEqual(len(current.playlists), 1)
        self.assertEqual(mock_get.call_count, 3)


if __name__ == "__main__":
    unittest.main()

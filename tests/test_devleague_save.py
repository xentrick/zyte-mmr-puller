import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

from zyte import devleague
from zyte.core import ZyteMMRPuller
from zyte.models import (
    CurrentMMRResult,
    PeakMMRByPlaylistResult,
    PeakMMRResult,
    PlaylistMMRRow,
    StandardProfile,
)


def _response(payload: object) -> MagicMock:
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = payload
    return response


class TestDevleagueSave(unittest.TestCase):
    def _load_profile(self) -> StandardProfile:
        payload_path = (
            Path(__file__).resolve().parent.parent / "json" / "standard_profile.json"
        )
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        return StandardProfile.model_validate(payload)

    def test_build_devleague_save_payload_shape_and_values(self) -> None:
        profile = self._load_profile()
        payload = ZyteMMRPuller.build_devleague_save_payload(
            profile=profile,
            tracker_link="https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview",
            pulled_by="tehblister",
            date_pulled="2026-04-02T16:51:59.416Z",
        )

        self.assertEqual(
            list(payload.keys()),
            [
                "from_api",
                "tracker_link",
                "threes_rating",
                "threes_games_played",
                "threes_season_peak",
                "twos_rating",
                "twos_games_played",
                "twos_season_peak",
                "ones_rating",
                "ones_games_played",
                "ones_season_peak",
                "notes",
                "date_pulled",
                "psyonix_season",
                "platform",
                "user_id",
                "pulled_by",
                "status",
            ],
        )

        self.assertEqual(
            payload["tracker_link"],
            {
                "link": "https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview"
            },
        )
        self.assertEqual(payload["threes_rating"], 1395)
        self.assertEqual(payload["threes_games_played"], 16)
        self.assertEqual(payload["twos_rating"], 1435)
        self.assertEqual(payload["twos_games_played"], 44)
        self.assertEqual(payload["ones_rating"], 919)
        self.assertEqual(payload["ones_games_played"], 4)
        self.assertEqual(payload["threes_season_peak"], 1470)
        self.assertEqual(payload["twos_season_peak"], 1504)
        self.assertEqual(payload["ones_season_peak"], 962)
        self.assertEqual(payload["platform"], "epic")
        self.assertEqual(
            payload["user_id"], profile.data.platformInfo.platformUserIdentifier
        )
        self.assertEqual(payload["pulled_by"], "tehblister")

    def test_build_devleague_save_payload_does_not_use_solo_standard(self) -> None:
        profile = self._load_profile()
        payload = ZyteMMRPuller.build_devleague_save_payload(
            profile=profile,
            tracker_link="https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview",
            pulled_by="tester",
            date_pulled="2026-04-02T16:51:59.416Z",
        )

        self.assertNotEqual(payload["threes_season_peak"], 476)
        self.assertEqual(payload["threes_season_peak"], 1470)

    @patch("zyte.devleague.requests.post")
    def test_post_devleague_payload_uses_http_post(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _response({"ok": True})
        payload = {
            "from_api": False,
            "tracker_link": {
                "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/nickm/overview"
            },
        }

        response = ZyteMMRPuller.post_devleague_payload(payload)

        self.assertEqual(response, {"ok": True})
        self.assertEqual(mock_post.call_args.args[0], ZyteMMRPuller.DEVLEAGUE_SAVE_URL)
        self.assertEqual(mock_post.call_args.kwargs["json"], payload)

    @patch("zyte.devleague.requests.post")
    def test_post_devleague_payload_timeout_returns_timeout_marker(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.side_effect = requests.Timeout("request timed out")
        payload = {
            "from_api": False,
            "tracker_link": {
                "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/nickm/overview"
            },
        }

        response = devleague.post_devleague_payload(payload)

        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        self.assertTrue(response.get("timeout"))

    def test_build_devleague_peak_season_payloads_maps_by_peak_season(self) -> None:
        profile = self._load_profile()
        current = CurrentMMRResult(
            season=36,
            playlists=[
                PlaylistMMRRow(
                    playlist_id=13,
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=1395,
                    games_played=16,
                ),
                PlaylistMMRRow(
                    playlist_id=11,
                    playlist_name="Ranked Doubles 2v2",
                    rank_rating=1435,
                    games_played=44,
                ),
                PlaylistMMRRow(
                    playlist_id=10,
                    playlist_name="Ranked Duel 1v1",
                    rank_rating=919,
                    games_played=4,
                ),
            ],
        )
        peaks = PeakMMRByPlaylistResult(
            playlists=[
                PeakMMRResult(
                    season=35,
                    playlist_id=13,
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=1470,
                    games_played=128,
                ),
                PeakMMRResult(
                    season=36,
                    playlist_id=11,
                    playlist_name="Ranked Doubles 2v2",
                    rank_rating=1504,
                ),
            ]
        )

        payloads = devleague.build_devleague_peak_season_payloads(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link="https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview",
            pulled_by="tester",
            date_pulled="2026-04-02T16:51:59.416Z",
        )

        self.assertEqual([p["psyonix_season"] for p in payloads], [36, 35])

        season_36 = payloads[0]
        self.assertEqual(season_36["twos_season_peak"], 1504)
        self.assertEqual(season_36["twos_rating"], 1435)
        self.assertEqual(season_36["twos_games_played"], 44)
        self.assertEqual(season_36["threes_season_peak"], 0)
        self.assertEqual(season_36["threes_rating"], 0)
        self.assertEqual(season_36["ones_season_peak"], 0)
        self.assertEqual(season_36["ones_rating"], 0)

        season_35 = payloads[1]
        self.assertEqual(season_35["threes_season_peak"], 1470)
        self.assertEqual(season_35["threes_rating"], 0)
        self.assertEqual(season_35["threes_games_played"], 128)
        self.assertEqual(season_35["twos_season_peak"], 0)
        self.assertEqual(season_35["ones_season_peak"], 0)

    def test_build_bad_tracker_payload_uses_tracker_link_for_current_page(self) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        payload = devleague.build_bad_tracker_payload(
            pulled_by="tester",
            tracker_link=tracker_link,
        )

        self.assertEqual(payload["pulled_by"], "tester")
        self.assertEqual(payload["tracker_link"], tracker_link)
        self.assertEqual(payload["current_page"], tracker_link)

    def test_build_devleague_save_payload_clamps_negative_mmr_to_zero(self) -> None:
        profile = self._load_profile()
        current = CurrentMMRResult(
            season=36,
            playlists=[
                PlaylistMMRRow(
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=-10,
                ),
                PlaylistMMRRow(
                    playlist_name="Ranked Doubles 2v2",
                    rank_rating=1234,
                ),
            ],
        )
        peaks = PeakMMRByPlaylistResult(
            playlists=[
                PeakMMRResult(
                    season=36,
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=-50,
                ),
                PeakMMRResult(
                    season=36,
                    playlist_name="Ranked Doubles 2v2",
                    rank_rating=1500,
                ),
            ]
        )

        payload = devleague.build_devleague_save_payload(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link="https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview",
            pulled_by="tester",
            date_pulled="2026-04-02T16:51:59.416Z",
        )

        self.assertEqual(payload["threes_rating"], 0)
        self.assertEqual(payload["threes_season_peak"], 0)
        self.assertEqual(payload["twos_rating"], 1234)
        self.assertEqual(payload["twos_season_peak"], 1500)

    def test_build_devleague_peak_season_payloads_clamps_negative_mmr_to_zero(
        self,
    ) -> None:
        profile = self._load_profile()
        current = CurrentMMRResult(
            season=36,
            playlists=[
                PlaylistMMRRow(
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=-4,
                    games_played=10,
                ),
            ],
        )
        peaks = PeakMMRByPlaylistResult(
            playlists=[
                PeakMMRResult(
                    season=36,
                    playlist_name="Ranked Standard 3v3",
                    rank_rating=-1,
                    games_played=20,
                )
            ]
        )

        payloads = devleague.build_devleague_peak_season_payloads(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link="https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview",
            pulled_by="tester",
            date_pulled="2026-04-02T16:51:59.416Z",
        )

        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["threes_rating"], 0)
        self.assertEqual(payloads[0]["threes_season_peak"], 0)

    @patch("zyte.devleague.requests.post")
    def test_post_bad_tracker_payload_uses_http_post(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.return_value = _response({"ok": True})
        payload = {
            "pulled_by": "tester",
            "tracker_link": "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview",
            "current_page": "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview",
        }

        response = devleague.post_bad_tracker_payload(payload)

        self.assertEqual(response, {"ok": True})
        self.assertEqual(
            mock_post.call_args.args[0], devleague.DEVLEAGUE_BAD_TRACKER_URL
        )
        self.assertEqual(mock_post.call_args.kwargs["json"], payload)


if __name__ == "__main__":
    unittest.main()

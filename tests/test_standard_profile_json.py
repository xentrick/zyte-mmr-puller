import json
import unittest
from pathlib import Path

from zyte.core import ZyteMMRPuller
from zyte.models import StandardProfile


class TestStandardProfileJson(unittest.TestCase):
    def _load_profile(self) -> StandardProfile:
        payload_path = (
            Path(__file__).resolve().parent.parent / "json" / "standard_profile.json"
        )
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        return StandardProfile.model_validate(payload)

    def test_standard_profile_parses(self) -> None:
        profile = self._load_profile()

        self.assertEqual(profile.data.metadata.currentSeason, 36)
        self.assertEqual(profile.data.platformInfo.platformSlug, "epic")
        self.assertEqual(len(profile.data.availableSegments), 36)
        self.assertGreater(len(profile.data.segments), 0)

    def test_playlist_ranked_by_season(self) -> None:
        profile = self._load_profile()

        parsed = ZyteMMRPuller.parse_playlist_ranked_by_season(profile)

        self.assertIn(36, parsed)
        self.assertEqual(len(parsed[36]), 9)

        doubles = next(item for item in parsed[36] if item["playlist_id"] == 11)
        self.assertEqual(doubles["playlist_name"], "Ranked Doubles 2v2")
        self.assertEqual(doubles["games_played"], 44)
        self.assertEqual(doubles["rank_rating"], 1435)
        self.assertEqual(doubles["rank_tier"], "Champion III")
        self.assertEqual(doubles["rank_division"], "Division IV")

    def test_peak_rank_by_season(self) -> None:
        profile = self._load_profile()

        parsed = ZyteMMRPuller.parse_peak_rank_by_season(profile)

        self.assertIn(29, parsed)
        self.assertEqual(len(parsed[29]), 3)

        doubles_peak = next(item for item in parsed[29] if item["playlist_id"] == 11)
        self.assertEqual(doubles_peak["playlist_name"], "Ranked Doubles 2v2")
        self.assertEqual(doubles_peak["games_played"], None)
        self.assertEqual(doubles_peak["rank_rating"], 1504)
        self.assertEqual(doubles_peak["rank_tier"], "Grand Champion I")
        self.assertEqual(doubles_peak["rank_division"], "Division III")

    def test_combined_rank_summary(self) -> None:
        profile = self._load_profile()

        parsed = ZyteMMRPuller.parse_rank_summary(profile)

        self.assertIn("playlist_ranked_by_season", parsed)
        self.assertIn("peak_rank_by_season", parsed)
        self.assertIn(36, parsed["playlist_ranked_by_season"])
        self.assertIn(29, parsed["peak_rank_by_season"])

    def test_get_current_mmr_latest_season(self) -> None:
        profile = self._load_profile()

        parsed = ZyteMMRPuller.get_current_mmr_latest_season(profile)

        self.assertEqual(parsed.season, 36)
        self.assertEqual(len(parsed.playlists), 9)

        doubles = next(item for item in parsed.playlists if item.playlist_id == 11)
        self.assertEqual(doubles.rank_rating, 1435)
        self.assertEqual(doubles.games_played, 44)

    def test_get_user_peak_mmr(self) -> None:
        profile = self._load_profile()

        parsed = ZyteMMRPuller.get_user_peak_mmr(profile)

        self.assertEqual(len(parsed.playlists), 10)

        doubles = next(item for item in parsed.playlists if item.playlist_id == 11)
        self.assertEqual(doubles.season, 29)
        self.assertEqual(doubles.rank_rating, 1504)

        casual = next(item for item in parsed.playlists if item.playlist_id == 0)
        self.assertEqual(casual.season, 17)
        self.assertEqual(casual.rank_rating, 1802)

    def test_get_player_id(self) -> None:
        profile = self._load_profile()

        player_id = ZyteMMRPuller.get_player_id(profile)

        self.assertEqual(player_id, 21631477)

    def test_profile_accepts_playlist_name_metadata_key(self) -> None:
        payload = {
            "data": {
                "availableSegments": [],
                "expiryDate": "2026-04-02T00:00:00+00:00",
                "metadata": {
                    "currentSeason": 35,
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
                "segments": [
                    {
                        "attributes": {"playlistId": 11, "season": 35},
                        "expiryDate": "0001-01-01T00:00:00+00:00",
                        "metadata": {"playlistName": "Ranked Doubles 2v2"},
                        "stats": {},
                        "type": "playlist",
                    }
                ],
            }
        }

        profile = StandardProfile.model_validate(payload)
        self.assertEqual(profile.data.segments[0].metadata.name, "Ranked Doubles 2v2")


if __name__ == "__main__":
    unittest.main()

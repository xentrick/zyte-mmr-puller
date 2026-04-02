import json
import unittest
from pathlib import Path

from zyte.core import ZyteMMRPuller


class TestTrackerLinkConversion(unittest.TestCase):
    def test_single_link_to_api_url(self) -> None:
        link = (
            "https://rocketleague.tracker.network/rocket-league/profile/steam/"
            "76561198203535048?utm_source=recentgames&utm_medium=link&utm_campaign=recentgames"
        )
        converted = ZyteMMRPuller.tracker_link_to_api_url(link)
        self.assertEqual(
            converted,
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/"
            "steam/76561198203535048",
        )

    def test_comma_separated_link_uses_first_profile_link(self) -> None:
        link = (
            "https://rocketleague.tracker.network/rocket-league/profile/epic/F16_TANNER/overview"
            " , https://rocketleague.tracker.network/rocket-league/profile/steam/76561198842199778/overview"
        )
        converted = ZyteMMRPuller.tracker_link_to_api_url(link)
        self.assertEqual(
            converted,
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/epic/F16_TANNER",
        )

    def test_xbl_is_normalized_to_xbox(self) -> None:
        link = "https://rocketleague.tracker.network/rocket-league/profile/xbl/Zar235"
        converted = ZyteMMRPuller.tracker_link_to_api_url(link)
        self.assertEqual(
            converted,
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/xbox/Zar235",
        )

    def test_batch_items_to_api_urls(self) -> None:
        payload_path = (
            Path(__file__).resolve().parent.parent / "json" / "trackers_next.json"
        )
        items = json.loads(payload_path.read_text(encoding="utf-8"))

        converted = ZyteMMRPuller.tracker_items_to_api_urls(items)

        self.assertGreater(len(converted), 0)
        self.assertIn(
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/76561198203535048",
            converted,
        )

    def test_normalize_cli_profile_url_missing_scheme(self) -> None:
        normalized, error = ZyteMMRPuller.normalize_cli_profile_url(
            "rocketleague.tracker.network/rocket-league/profile/steam/76561198203535048"
        )
        self.assertIsNone(error)
        self.assertEqual(
            normalized,
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/76561198203535048",
        )

    def test_normalize_cli_profile_url_api_passthrough(self) -> None:
        url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/76561198203535048"
        normalized, error = ZyteMMRPuller.normalize_cli_profile_url(url)
        self.assertIsNone(error)
        self.assertEqual(normalized, url)

    def test_normalize_cli_profile_url_malformed_tracker(self) -> None:
        normalized, error = ZyteMMRPuller.normalize_cli_profile_url(
            "https://rocketleague.tracker.network/rocket-league/profile/steam"
        )
        self.assertIsNone(normalized)
        self.assertIsNotNone(error)

    def test_normalize_cli_profile_url_unsupported_host(self) -> None:
        normalized, error = ZyteMMRPuller.normalize_cli_profile_url(
            "https://example.com/profile/steam/abc"
        )
        self.assertIsNone(error)
        self.assertEqual(
            normalized,
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/abc",
        )

    def test_normalize_cli_profile_url_incomplete_profile_path(self) -> None:
        normalized, error = ZyteMMRPuller.normalize_cli_profile_url(
            "https://example.com/profile/steam"
        )
        self.assertIsNone(normalized)
        self.assertIsNotNone(error)


if __name__ == "__main__":
    unittest.main()

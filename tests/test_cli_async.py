import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, MagicMock, patch

from zyte import __main__ as cli_main


class _Dumpable:
    def __init__(self, payload: dict):
        self.payload = payload

    def model_dump(self, mode: str = "json") -> dict:
        return self.payload


class TestCliAsync(unittest.TestCase):
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_current_uses_async_pull(self, mock_puller_cls: MagicMock) -> None:
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        argv = [
            "zyte",
            "current",
            api_url,
            "--api-key",
            "test",
            "--async",
        ]

        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)
        mock_puller_cls.get_current_mmr_latest_season.return_value = _Dumpable(
            {"season": 36, "playlists": []}
        )

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=object())
        instance.pull_mmr = MagicMock()

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.pull_mmr_async.assert_awaited_once_with(
            api_url,
            http_response_body=True,
        )
        instance.pull_mmr.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["season"], 36)

    @patch("zyte.__main__.ZyteMMRPuller")
    def test_season_uses_async_pull(self, mock_puller_cls: MagicMock) -> None:
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        argv = [
            "zyte",
            "season",
            api_url,
            "35",
            "--api-key",
            "test",
            "--async",
        ]

        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.pull_season_async = AsyncMock(
            return_value=_Dumpable({"data": [{"season": 35}]})
        )
        instance.pull_season = MagicMock()

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.pull_season_async.assert_awaited_once_with(
            api_url,
            season=35,
            http_response_body=True,
        )
        instance.pull_season.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["data"][0]["season"], 35)


if __name__ == "__main__":
    unittest.main()

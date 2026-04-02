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

    @patch("zyte.__main__.ZyteMMRPuller")
    def test_season_peaks_uses_async_pull(self, mock_puller_cls: MagicMock) -> None:
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        argv = [
            "zyte",
            "season-peaks",
            api_url,
            "36",
            "--window",
            "5",
            "--api-key",
            "test",
            "--async",
        ]

        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.get_peak_mmr_by_recent_seasons_async = AsyncMock(
            return_value=_Dumpable(
                {
                    "playlists": [
                        {
                            "playlist_id": 11,
                            "playlist_name": "Ranked Doubles 2v2",
                            "season": 35,
                            "rank_rating": 1510,
                        }
                    ]
                }
            )
        )

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.get_peak_mmr_by_recent_seasons_async.assert_awaited_once_with(
            api_url,
            start_season=36,
            seasons_to_scan=5,
            http_response_body=True,
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["playlists"][0]["playlist_id"], 11)

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_sync_pipeline(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post: MagicMock,
    ) -> None:
        tracker_next_url = "https://example.com/next"
        devleague_url = "https://example.com/save"
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--tracker-next-url",
            tracker_next_url,
            "--devleague-url",
            devleague_url,
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)
        instance.pull_mmr_async = AsyncMock()

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_puller_cls.pull_next_tracker_links.assert_called_once_with(
            limit=5,
            endpoint_url=tracker_next_url,
        )
        instance.pull_mmr.assert_called_once_with(api_url, http_response_body=True)
        instance.pull_mmr_async.assert_not_called()
        mock_puller_cls.get_user_peak_mmr.assert_called_once_with(profile)
        mock_build_payloads.assert_called_once_with(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link=tracker_link,
            pulled_by="tester",
            notes="Automated pull by nickm",
            from_api=False,
            status=None,
        )
        mock_devleague_post.assert_called_once_with(
            payload={"payload": "built"},
            endpoint_url=devleague_url,
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["requested_limit"], 5)
        self.assertEqual(payload["posted_count"], 1)
        self.assertEqual(payload["failed_count"], 0)
        self.assertEqual(
            payload["results"][0]["devleague_payloads"], [{"payload": "built"}]
        )
        self.assertEqual(payload["results"][0]["devleague_responses"], [{"ok": True}])

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_sync_uses_devleague_tracker_source(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--tracker-source",
            "devleague",
        ]

        mock_puller_cls.pull_devleague_tracker_links.return_value = [
            {"link": tracker_link}
        ]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_puller_cls.pull_devleague_tracker_links.assert_called_once_with(limit=5)

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_uses_devleague_tracker_source(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--tracker-source",
            "devleague",
        ]

        mock_puller_cls.pull_devleague_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_puller_cls.pull_devleague_tracker_links_async.assert_awaited_once_with(
            limit=5,
        )

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_sync_multiple_batches(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post: MagicMock,
    ) -> None:
        tracker_next_url = "https://example.com/next"
        devleague_url = "https://example.com/save"
        tracker_link_1 = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        tracker_link_2 = "https://rocketleague.tracker.network/rocket-league/profile/steam/2/overview"
        api_url_1 = (
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        )
        api_url_2 = (
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/2"
        )

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--tracker-next-url",
            tracker_next_url,
            "--devleague-url",
            devleague_url,
            "--batches",
            "2",
        ]

        mock_puller_cls.pull_next_tracker_links.side_effect = [
            [{"link": tracker_link_1}],
            [{"link": tracker_link_2}],
        ]
        mock_puller_cls.normalize_cli_profile_url.side_effect = [
            (api_url_1, None),
            (api_url_2, None),
        ]

        profile_1 = object()
        profile_2 = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr.side_effect = [profile_1, profile_2]
        instance.pull_mmr_async = AsyncMock()

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        self.assertEqual(mock_puller_cls.pull_next_tracker_links.call_count, 2)
        self.assertEqual(instance.pull_mmr.call_count, 2)
        self.assertEqual(mock_devleague_post.call_count, 2)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["requested_limit"], 5)
        self.assertEqual(payload["requested_batches"], 2)
        self.assertEqual(payload["pulled_count"], 2)
        self.assertEqual(payload["posted_count"], 2)

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_sync_uses_recent_season_peaks_when_requested(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--use-recent-season-peaks",
            "--window",
            "3",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = MagicMock(season=36)
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)
        instance.get_current_mmr_by_window = MagicMock(return_value=current)
        instance.get_peak_mmr_by_recent_seasons = MagicMock(return_value=peaks)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr = MagicMock()
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.get_current_mmr_by_window.assert_called_once_with(
            api_url,
            start_season=36,
            seasons_to_scan=3,
            http_response_body=True,
        )
        instance.get_peak_mmr_by_recent_seasons.assert_called_once_with(
            api_url,
            start_season=36,
            seasons_to_scan=3,
            http_response_body=True,
        )
        mock_puller_cls.get_user_peak_mmr.assert_not_called()
        mock_build_payloads.assert_called_once()

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_uses_recent_season_peaks_when_requested(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--use-recent-season-peaks",
            "--window",
            "4",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = MagicMock(season=36)
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=profile)
        instance.get_current_mmr_by_window_async = AsyncMock(return_value=current)
        instance.get_peak_mmr_by_recent_seasons_async = AsyncMock(return_value=peaks)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr = MagicMock()
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.get_current_mmr_by_window_async.assert_awaited_once_with(
            api_url,
            start_season=36,
            seasons_to_scan=4,
            http_response_body=True,
        )
        instance.get_peak_mmr_by_recent_seasons_async.assert_awaited_once_with(
            api_url,
            start_season=36,
            seasons_to_scan=4,
            http_response_body=True,
        )
        mock_puller_cls.get_user_peak_mmr.assert_not_called()
        mock_build_payloads.assert_called_once()

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_sync_uses_specific_season_when_requested(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--season",
            "35",
            "--window",
            "9",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)
        instance.get_current_mmr_by_season = MagicMock(return_value=current)
        instance.get_peak_mmr_by_recent_seasons = MagicMock(return_value=peaks)

        mock_puller_cls.get_current_mmr_latest_season = MagicMock()
        mock_puller_cls.get_user_peak_mmr = MagicMock()
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.get_current_mmr_by_season.assert_called_once_with(
            api_url,
            season=35,
            http_response_body=True,
        )
        instance.get_peak_mmr_by_recent_seasons.assert_called_once_with(
            api_url,
            start_season=35,
            seasons_to_scan=1,
            http_response_body=True,
        )
        mock_puller_cls.get_current_mmr_latest_season.assert_not_called()
        mock_puller_cls.get_user_peak_mmr.assert_not_called()

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_uses_specific_season_when_requested(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--season",
            "35",
            "--window",
            "9",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=profile)
        instance.get_current_mmr_by_season_async = AsyncMock(return_value=current)
        instance.get_peak_mmr_by_recent_seasons_async = AsyncMock(return_value=peaks)

        mock_puller_cls.get_current_mmr_latest_season = MagicMock()
        mock_puller_cls.get_user_peak_mmr = MagicMock()
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        instance.get_current_mmr_by_season_async.assert_awaited_once_with(
            api_url,
            season=35,
            http_response_body=True,
        )
        instance.get_peak_mmr_by_recent_seasons_async.assert_awaited_once_with(
            api_url,
            start_season=35,
            seasons_to_scan=1,
            http_response_body=True,
        )
        mock_puller_cls.get_current_mmr_latest_season.assert_not_called()
        mock_puller_cls.get_user_peak_mmr.assert_not_called()

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_pipeline(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        devleague_url = "https://example.com/save"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--from-api",
            "--devleague-url",
            devleague_url,
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.pull_next_tracker_links = MagicMock()
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock()
        instance.pull_mmr_async = AsyncMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_puller_cls.pull_next_tracker_links.assert_not_called()
        mock_puller_cls.pull_next_tracker_links_async.assert_awaited_once_with(
            limit=5,
            endpoint_url=mock_puller_cls.TRACKER_NEXT_URL,
        )
        instance.pull_mmr.assert_not_called()
        instance.pull_mmr_async.assert_awaited_once_with(
            api_url,
            http_response_body=True,
        )
        mock_build_payloads.assert_called_once()
        self.assertTrue(mock_build_payloads.call_args.kwargs["from_api"])
        mock_devleague_post_async.assert_awaited_once_with(
            payload={"payload": "built"},
            endpoint_url=devleague_url,
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["posted_count"], 1)
        self.assertEqual(
            payload["results"][0]["devleague_payloads"], [{"payload": "built"}]
        )
        self.assertEqual(payload["results"][0]["devleague_responses"], [{"ok": True}])

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_multiple_batches(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link_1 = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        tracker_link_2 = "https://rocketleague.tracker.network/rocket-league/profile/steam/2/overview"
        api_url_1 = (
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"
        )
        api_url_2 = (
            "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/2"
        )

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--batches",
            "2",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            side_effect=[
                [{"link": tracker_link_1}],
                [{"link": tracker_link_2}],
            ]
        )
        mock_puller_cls.normalize_cli_profile_url.side_effect = [
            (api_url_1, None),
            (api_url_2, None),
        ]

        profile_1 = object()
        profile_2 = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock()
        instance.pull_mmr_async = AsyncMock(side_effect=[profile_1, profile_2])

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        self.assertEqual(mock_puller_cls.pull_next_tracker_links_async.await_count, 2)
        self.assertEqual(instance.pull_mmr_async.await_count, 2)
        self.assertEqual(mock_devleague_post_async.await_count, 2)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["requested_limit"], 5)
        self.assertEqual(payload["requested_batches"], 2)
        self.assertEqual(payload["pulled_count"], 2)
        self.assertEqual(payload["posted_count"], 2)

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_dry_run_builds_payload_without_posting(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_payload: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--dry-run",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"notes": "Automated pull by nickm"}]

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_called_once_with(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link=tracker_link,
            pulled_by="tester",
            notes="Automated pull by nickm",
            from_api=False,
            status=None,
        )
        mock_devleague_post_payload.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["dry_run_count"], 1)
        self.assertEqual(payload["posted_count"], 0)
        self.assertEqual(payload["results"][0]["status"], "dry-run")
        self.assertIn("devleague_payloads", payload["results"][0])

    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_marks_timeout_and_continues_sync(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_payload: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_payload.return_value = {"timeout": True, "error": "timeout"}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertTrue(payload["results"][0]["post_timeout"])
        self.assertEqual(
            payload["results"][0]["error"],
            "Timed out posting payload to DevLeague",
        )

    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_marks_timeout_and_continues_async(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/1/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/1"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = object()
        peaks = object()

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=profile)

        mock_puller_cls.get_current_mmr_latest_season.return_value = current
        mock_puller_cls.get_user_peak_mmr.return_value = peaks
        mock_build_payloads.return_value = [{"payload": "built"}]
        mock_devleague_post_async.return_value = {"timeout": True, "error": "timeout"}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertTrue(payload["results"][0]["post_timeout"])
        self.assertEqual(
            payload["results"][0]["error"],
            "Timed out posting payload to DevLeague",
        )

    @patch("zyte.__main__.devleague.post_bad_tracker_payload")
    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_reports_bad_tracker_for_missing_profile(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_payload: MagicMock,
        mock_bad_tracker_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/missing-user"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=None)
        mock_bad_tracker_post.return_value = {"ok": True}

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_not_called()
        mock_devleague_post_payload.assert_not_called()
        mock_bad_tracker_post.assert_called_once()

        bad_tracker_payload = mock_bad_tracker_post.call_args.kwargs["payload"]
        self.assertEqual(bad_tracker_payload["pulled_by"], "tester")
        self.assertEqual(bad_tracker_payload["tracker_link"], tracker_link)
        self.assertEqual(bad_tracker_payload["current_page"], tracker_link)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["posted_count"], 0)
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["skipped_count"], 0)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertTrue(payload["results"][0]["bad_tracker_reported"])

    @patch("zyte.__main__.devleague.post_bad_tracker_payload")
    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_dry_run_missing_profile_does_not_post_bad_tracker(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_payload: MagicMock,
        mock_bad_tracker_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/missing-user"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--dry-run",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=None)

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_not_called()
        mock_devleague_post_payload.assert_not_called()
        mock_bad_tracker_post.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertFalse(payload["results"][0]["bad_tracker_reported"])
        self.assertEqual(
            payload["results"][0]["bad_tracker_payload_preview"],
            {
                "pulled_by": "tester",
                "tracker_link": tracker_link,
                "current_page": tracker_link,
            },
        )

    @patch("zyte.__main__.devleague.post_bad_tracker_payload")
    @patch("zyte.__main__.devleague.post_devleague_payload")
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_no_post_failed_trackers_flag_skips_bad_tracker_post(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_payload: MagicMock,
        mock_bad_tracker_post: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/missing-user"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--no-post-failed-trackers",
        ]

        mock_puller_cls.pull_next_tracker_links.return_value = [{"link": tracker_link}]
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.pull_mmr = MagicMock(return_value=None)

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_not_called()
        mock_devleague_post_payload.assert_not_called()
        mock_bad_tracker_post.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertFalse(payload["results"][0]["bad_tracker_reported"])
        self.assertEqual(
            payload["results"][0]["bad_tracker_payload_preview"],
            {
                "pulled_by": "tester",
                "tracker_link": tracker_link,
                "current_page": tracker_link,
            },
        )

    @patch(
        "zyte.__main__.devleague.post_bad_tracker_payload_async",
        new_callable=AsyncMock,
    )
    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_no_post_failed_trackers_flag_skips_bad_tracker_post(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
        mock_bad_tracker_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/missing-user"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--no-post-failed-trackers",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=None)

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_not_called()
        mock_devleague_post_async.assert_not_called()
        mock_bad_tracker_post_async.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertFalse(payload["results"][0]["bad_tracker_reported"])
        self.assertEqual(
            payload["results"][0]["bad_tracker_payload_preview"],
            {
                "pulled_by": "tester",
                "tracker_link": tracker_link,
                "current_page": tracker_link,
            },
        )

    @patch(
        "zyte.__main__.devleague.post_bad_tracker_payload_async",
        new_callable=AsyncMock,
    )
    @patch(
        "zyte.__main__.devleague.post_devleague_payload_async",
        new_callable=AsyncMock,
    )
    @patch("zyte.__main__.devleague.build_devleague_peak_season_payloads")
    @patch("zyte.__main__.ZyteMMRPuller")
    def test_post_next_peaks_async_recent_season_peak_error_is_failed_not_crash(
        self,
        mock_puller_cls: MagicMock,
        mock_build_payloads: MagicMock,
        mock_devleague_post_async: MagicMock,
        mock_bad_tracker_post_async: MagicMock,
    ) -> None:
        tracker_link = "https://rocketleague.tracker.network/rocket-league/profile/steam/missing-user/overview"
        api_url = "https://api.tracker.gg/api/v2/rocket-league/standard/profile/steam/missing-user"

        argv = [
            "zyte",
            "post-next-peaks",
            "--api-key",
            "test",
            "--pulled-by",
            "tester",
            "--async",
            "--use-recent-season-peaks",
            "--no-post-failed-trackers",
        ]

        mock_puller_cls.pull_next_tracker_links_async = AsyncMock(
            return_value=[{"link": tracker_link}]
        )
        mock_puller_cls.normalize_cli_profile_url.return_value = (api_url, None)

        profile = object()
        current = MagicMock(season=36)

        instance = mock_puller_cls.return_value
        instance.pull_mmr_async = AsyncMock(return_value=profile)
        instance.get_current_mmr_by_window_async = AsyncMock(return_value=current)
        instance.get_peak_mmr_by_recent_seasons_async = AsyncMock(
            side_effect=RuntimeError("boom")
        )

        mock_puller_cls.get_current_mmr_latest_season.return_value = current

        stdout = io.StringIO()
        with patch.object(sys, "argv", argv), redirect_stdout(stdout):
            cli_main.main()

        mock_build_payloads.assert_not_called()
        mock_devleague_post_async.assert_not_called()
        mock_bad_tracker_post_async.assert_not_called()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["failed_count"], 1)
        self.assertEqual(payload["results"][0]["status"], "failed")
        self.assertIn("Peak pull error:", payload["results"][0]["error"])
        self.assertFalse(payload["results"][0]["bad_tracker_reported"])
        self.assertEqual(
            payload["results"][0]["bad_tracker_payload_preview"],
            {
                "pulled_by": "tester",
                "tracker_link": tracker_link,
                "current_page": tracker_link,
            },
        )


if __name__ == "__main__":
    unittest.main()

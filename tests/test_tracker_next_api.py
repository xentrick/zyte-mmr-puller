import unittest
from unittest.mock import MagicMock, patch

import requests

from zyte.core import TrackerSourcePullError, ZyteMMRPuller


def _response(payload: object) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


class TestTrackerNextApi(unittest.TestCase):
    @patch("zyte.core.requests.get")
    def test_pull_next_tracker_links_list_payload(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _response(
            [
                {
                    "id": 1,
                    "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/111",
                },
                {
                    "id": 2,
                    "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/222",
                },
            ]
        )

        rows = ZyteMMRPuller.pull_next_tracker_links(limit=20)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], 1)
        request_url = mock_get.call_args.args[0]
        self.assertIn("limit=20", request_url)

    @patch("zyte.core.requests.get")
    def test_pull_next_tracker_links_results_payload(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _response(
            {
                "results": [
                    {
                        "id": 3,
                        "link": "https://rocketleague.tracker.network/rocket-league/profile/epic/F16",
                    }
                ]
            }
        )

        rows = ZyteMMRPuller.pull_next_tracker_links(limit=5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], 3)

    @patch("zyte.core.requests.get")
    def test_pull_next_tracker_links_request_error_is_fatal(
        self, mock_get: MagicMock
    ) -> None:
        mock_get.side_effect = requests.RequestException("Bad Gateway")

        with self.assertRaises(TrackerSourcePullError):
            ZyteMMRPuller.pull_next_tracker_links(limit=1)

    @patch("zyte.core.requests.get")
    def test_pull_devleague_tracker_links_tracker_object_payload(
        self, mock_get: MagicMock
    ) -> None:
        mock_get.return_value = _response(
            {
                "version": "3.2.6",
                "tracker": {
                    "link": "https://rocketleague.tracker.network/rocket-league/profile/epic/Prodijoe/mmr",
                    "id": 14454,
                    "name": "Prodijoe",
                },
                "remaining": 8,
            }
        )

        rows = ZyteMMRPuller.pull_devleague_tracker_links(limit=1)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], 14454)
        self.assertEqual(
            rows[0]["link"],
            "https://rocketleague.tracker.network/rocket-league/profile/epic/Prodijoe/mmr",
        )
        self.assertEqual(mock_get.call_count, 1)

    @patch("zyte.core.requests.get")
    def test_pull_devleague_tracker_links_retries_until_limit_or_remaining(
        self, mock_get: MagicMock
    ) -> None:
        mock_get.side_effect = [
            _response(
                {
                    "tracker": {
                        "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/111/mmr",
                        "id": 1,
                    },
                    "remaining": 3,
                }
            ),
            _response(
                {
                    "tracker": {
                        "link": "https://rocketleague.tracker.network/rocket-league/profile/steam/222/mmr",
                        "id": 2,
                    },
                    "remaining": 0,
                }
            ),
        ]

        rows = ZyteMMRPuller.pull_devleague_tracker_links(limit=5)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], 1)
        self.assertEqual(rows[1]["id"], 2)
        self.assertEqual(mock_get.call_count, 2)

    @patch("zyte.core.requests.get")
    def test_pull_devleague_tracker_links_request_error_is_fatal(
        self, mock_get: MagicMock
    ) -> None:
        mock_get.side_effect = requests.RequestException("Bad Gateway")

        with self.assertRaises(TrackerSourcePullError):
            ZyteMMRPuller.pull_devleague_tracker_links(limit=1)

    @patch("zyte.core.ZyteMMRPuller.pull_next_tracker_links")
    def test_iter_next_tracker_links_stops_on_empty(self, mock_pull: MagicMock) -> None:
        mock_pull.side_effect = [
            [{"id": 1}, {"id": 2}],
            [],
        ]

        rows = list(ZyteMMRPuller.iter_next_tracker_links(limit=2, stop_on_empty=True))

        self.assertEqual(rows, [{"id": 1}, {"id": 2}])
        self.assertEqual(mock_pull.call_count, 2)

    @patch("zyte.core.time.sleep")
    @patch("zyte.core.ZyteMMRPuller.pull_next_tracker_links")
    def test_iter_next_tracker_links_can_poll_continuously(
        self, mock_pull: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_pull.side_effect = [
            [],
            [{"id": 9}],
            [],
        ]

        rows = list(
            ZyteMMRPuller.iter_next_tracker_links(
                limit=1,
                stop_on_empty=False,
                poll_interval_seconds=0.25,
                max_batches=3,
            )
        )

        self.assertEqual(rows, [{"id": 9}])
        self.assertEqual(mock_pull.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 3)


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path

from zyte.segment_models import SegmentPlaylistResponse


class TestSegmentPlaylistJson(unittest.TestCase):
    def test_segment_playlist_parses(self) -> None:
        payload_path = Path(__file__).resolve().parent.parent / "json" / "segment.json"
        payload = json.loads(payload_path.read_text(encoding="utf-8"))

        parsed = SegmentPlaylistResponse.model_validate(payload)

        self.assertGreater(len(parsed.data), 0)

        first = parsed.data[0]
        self.assertEqual(first.type, "playlist")
        self.assertEqual(first.attributes.season, 35)
        self.assertEqual(first.attributes.playlistId, 11)
        self.assertEqual(first.metadata.name, "Ranked Doubles 2v2")

        self.assertIn("rating", first.stats)
        self.assertEqual(first.stats["rating"].value, 1505)


if __name__ == "__main__":
    unittest.main()

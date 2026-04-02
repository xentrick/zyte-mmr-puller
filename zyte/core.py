import base64
import binascii
import logging
import json
from collections import defaultdict
from typing import Any
from urllib.parse import quote, unquote, urlparse

from pydantic import ValidationError
from zyte_api import ZyteAPI, AsyncZyteAPI

from zyte.models import (
    CurrentMMRResult,
    PeakMMRByPlaylistResult,
    PeakMMRResult,
    PlaylistMMRRow,
    StandardProfile,
)
from zyte.segment_models import SegmentPlaylistResponse

log = logging.getLogger(__name__)


class ZyteMMRPuller:
    API_PROFILE_BASE_URL = (
        "https://api.tracker.gg/api/v2/rocket-league/standard/profile"
    )
    TRACKER_PROFILE_HOSTS = {
        "rocketleague.tracker.network",
        "www.rocketleague.tracker.network",
    }
    API_HOST = "api.tracker.gg"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = ZyteAPI(api_key=self.api_key)
        self.async_client = AsyncZyteAPI(api_key=self.api_key)

    @staticmethod
    def _build_request_opts(url: str, http_response_body: bool) -> dict[str, Any]:
        return {"url": url, "httpResponseBody": http_response_body}

    @staticmethod
    def _decode_http_response_body(resp: dict[str, Any] | None) -> Any | None:
        if not resp:
            return None

        encoded_body = resp.get("httpResponseBody")
        if not encoded_body:
            return None

        try:
            decoded = base64.b64decode(encoded_body).decode("utf-8")
            return json.loads(decoded)
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
            log.exception("Failed to decode or parse httpResponseBody")
            return None

    @staticmethod
    def _is_api_error_payload(body: Any) -> bool:
        return bool(
            isinstance(body, dict) and body.get("errors") and "data" not in body
        )

    @staticmethod
    def _validate_standard_profile(body: Any) -> StandardProfile | None:
        if ZyteMMRPuller._is_api_error_payload(body):
            log.warning("Profile request returned API errors instead of data payload")
            return None

        try:
            return StandardProfile.model_validate(body)
        except ValidationError:
            log.exception("Failed to validate profile payload")
            return None

    @staticmethod
    def _validate_segment_playlist(body: Any) -> SegmentPlaylistResponse | None:
        if ZyteMMRPuller._is_api_error_payload(body):
            log.warning("Season request returned API errors instead of data payload")
            return None

        try:
            return SegmentPlaylistResponse.model_validate(body)
        except ValidationError:
            log.exception("Failed to validate season payload")
            return None

    def pull_mmr(
        self, url: str, http_response_body: bool = True
    ) -> StandardProfile | None:
        opts = self._build_request_opts(url, http_response_body)
        resp = self.client.get(opts)
        body = self._decode_http_response_body(resp)
        if body is None:
            return None
        return self._validate_standard_profile(body)

    async def pull_mmr_async(
        self, url: str, http_response_body: bool = True
    ) -> StandardProfile | None:
        opts = self._build_request_opts(url, http_response_body)
        resp = await self.async_client.get(opts)
        body = self._decode_http_response_body(resp)
        if body is None:
            return None
        return self._validate_standard_profile(body)

    def pull_season(
        self, url: str, season: int, http_response_body: bool = True
    ) -> SegmentPlaylistResponse | None:
        endpoint_url = f"{url.rstrip('/')}/segments/playlist?season={season}"
        opts = self._build_request_opts(endpoint_url, http_response_body)
        resp = self.client.get(opts)
        body = self._decode_http_response_body(resp)
        if body is None:
            return None
        return self._validate_segment_playlist(body)

    async def pull_season_async(
        self, url: str, season: int, http_response_body: bool = True
    ) -> SegmentPlaylistResponse | None:
        endpoint_url = f"{url.rstrip('/')}/segments/playlist?season={season}"
        opts = self._build_request_opts(endpoint_url, http_response_body)
        resp = await self.async_client.get(opts)
        body = self._decode_http_response_body(resp)
        if body is None:
            return None
        return self._validate_segment_playlist(body)

    @staticmethod
    def parse_playlist_ranked_by_season(
        profile: StandardProfile,
    ) -> dict[int, list[dict[str, Any]]]:
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)

        for segment in profile.data.segments:
            if getattr(segment, "type", None) != "playlist":
                continue

            season = segment.attributes.get("season")
            if not isinstance(season, int):
                continue

            playlist_id = segment.attributes.get("playlistId")
            matches = segment.stats.get("matchesPlayed")
            rating = segment.stats.get("rating")
            tier = segment.stats.get("tier")
            division = segment.stats.get("division")

            grouped[season].append(
                {
                    "playlist_id": playlist_id,
                    "playlist_name": segment.metadata.name,
                    "games_played": matches.value if matches else None,
                    "games_played_display": matches.displayValue if matches else None,
                    "rank_rating": rating.value if rating else None,
                    "rank_rating_display": rating.displayValue if rating else None,
                    "rank_tier": (
                        (tier.metadata.get("name") if tier else None)
                        or (rating.metadata.get("tierName") if rating else None)
                    ),
                    "rank_tier_value": tier.value if tier else None,
                    "rank_division": (
                        division.metadata.get("name") if division else None
                    ),
                    "rank_division_value": division.value if division else None,
                }
            )

        result = dict(sorted(grouped.items(), key=lambda kv: kv[0]))
        for rows in result.values():
            rows.sort(key=lambda row: (row["playlist_id"] is None, row["playlist_id"]))
        return result

    @staticmethod
    def parse_peak_rank_by_season(
        profile: StandardProfile,
    ) -> dict[int, list[dict[str, Any]]]:
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)

        for segment in profile.data.segments:
            if getattr(segment, "type", None) != "peak-rating":
                continue

            season = segment.attributes.get("season")
            if not isinstance(season, int):
                continue

            playlist_id = segment.attributes.get("playlistId")
            peak = segment.stats.get("peakRating")

            grouped[season].append(
                {
                    "playlist_id": playlist_id,
                    "playlist_name": segment.metadata.name,
                    "games_played": None,
                    "games_played_display": None,
                    "rank_rating": peak.value if peak else None,
                    "rank_rating_display": peak.displayValue if peak else None,
                    "rank_tier": peak.metadata.get("name") if peak else None,
                    "rank_tier_value": None,
                    "rank_division": peak.metadata.get("division") if peak else None,
                    "rank_division_value": None,
                }
            )

        result = dict(sorted(grouped.items(), key=lambda kv: kv[0]))
        for rows in result.values():
            rows.sort(key=lambda row: (row["playlist_id"] is None, row["playlist_id"]))
        return result

    @staticmethod
    def parse_rank_summary(
        profile: StandardProfile,
    ) -> dict[str, dict[int, list[dict[str, Any]]]]:
        return {
            "playlist_ranked_by_season": ZyteMMRPuller.parse_playlist_ranked_by_season(
                profile
            ),
            "peak_rank_by_season": ZyteMMRPuller.parse_peak_rank_by_season(profile),
        }

    @staticmethod
    def get_current_mmr_latest_season(profile: StandardProfile) -> CurrentMMRResult:
        playlist_by_season = ZyteMMRPuller.parse_playlist_ranked_by_season(profile)
        if not playlist_by_season:
            return CurrentMMRResult()

        season = profile.data.metadata.currentSeason
        if season not in playlist_by_season:
            season = max(playlist_by_season)

        playlists = [
            PlaylistMMRRow.model_validate(row)
            for row in playlist_by_season.get(season, [])
        ]
        return CurrentMMRResult(season=season, playlists=playlists)

    @staticmethod
    def get_user_peak_mmr(profile: StandardProfile) -> PeakMMRByPlaylistResult:
        peak_by_season = ZyteMMRPuller.parse_peak_rank_by_season(profile)
        best_by_playlist: dict[tuple[int | None, str], PeakMMRResult] = {}

        for season, rows in peak_by_season.items():
            for row in rows:
                value = row.get("rank_rating")
                if not isinstance(value, (int, float)):
                    continue

                candidate = PeakMMRResult.model_validate({"season": season, **row})
                key = (candidate.playlist_id, candidate.playlist_name)
                current = best_by_playlist.get(key)
                if current is None:
                    best_by_playlist[key] = candidate
                    continue

                current_rating = (
                    float(current.rank_rating)
                    if isinstance(current.rank_rating, (int, float))
                    else float("-inf")
                )
                candidate_rating = float(value)

                if candidate_rating > current_rating:
                    best_by_playlist[key] = candidate
                elif (
                    candidate_rating == current_rating
                    and candidate.season > current.season
                ):
                    best_by_playlist[key] = candidate

        playlists = sorted(
            best_by_playlist.values(),
            key=lambda row: (
                row.playlist_id is None,
                row.playlist_id,
                row.playlist_name,
            ),
        )
        return PeakMMRByPlaylistResult(playlists=playlists)

    @staticmethod
    def get_player_id(profile: StandardProfile) -> int:
        return profile.data.metadata.playerId

    @staticmethod
    def tracker_link_to_api_url(link: str) -> str | None:
        platform_aliases = {
            "xbl": "xbox",
        }

        for candidate in (part.strip() for part in link.split(",")):
            if not candidate:
                continue

            parsed = urlparse(candidate)
            path_parts = [unquote(part) for part in parsed.path.split("/") if part]

            try:
                profile_index = path_parts.index("profile")
            except ValueError:
                continue

            if profile_index + 2 >= len(path_parts):
                continue

            platform = path_parts[profile_index + 1].lower()
            platform = platform_aliases.get(platform, platform)
            player_identifier = path_parts[profile_index + 2]

            if not platform or not player_identifier:
                continue

            encoded_identifier = quote(player_identifier, safe="")
            return (
                f"{ZyteMMRPuller.API_PROFILE_BASE_URL}/{platform}/{encoded_identifier}"
            )

        return None

    @staticmethod
    def tracker_items_to_api_urls(items: list[dict[str, Any]]) -> list[str]:
        urls: list[str] = []
        for item in items:
            link = item.get("link")
            if not isinstance(link, str):
                continue

            api_url = ZyteMMRPuller.tracker_link_to_api_url(link)
            if api_url is not None:
                urls.append(api_url)

        return urls

    @staticmethod
    def normalize_cli_profile_url(url: str) -> tuple[str | None, str | None]:
        cleaned = url.strip().strip("`").strip("\"'")
        if not cleaned:
            return None, "Empty URL provided."

        candidate = cleaned
        parsed = urlparse(candidate)

        # Accept common malformed input that omits scheme.
        if not parsed.scheme and candidate.startswith(
            (
                "api.tracker.gg/",
                "rocketleague.tracker.network/",
                "www.rocketleague.tracker.network/",
            )
        ):
            candidate = f"https://{candidate}"
            parsed = urlparse(candidate)

        # If the input path has /profile/<platform>/<player>, convert it directly
        # regardless of hostname (tracker links are sometimes copied with custom hosts).
        converted = ZyteMMRPuller.tracker_link_to_api_url(candidate)
        if converted is not None:
            return converted, None

        host = parsed.netloc.lower()

        if host == ZyteMMRPuller.API_HOST:
            if parsed.path.startswith("/api/v2/rocket-league/standard/profile/"):
                return candidate, None
            return (
                None,
                "Unsupported api.tracker.gg URL. Expected /api/v2/rocket-league/standard/profile/<platform>/<player_identifier>.",
            )

        path_parts = [part for part in parsed.path.split("/") if part]
        if "profile" in path_parts:
            return (
                None,
                "Malformed profile URL. Expected .../profile/<platform>/<player_identifier>.",
            )

        return (
            None,
            "Unsupported URL host. Use a rocketleague.tracker.network profile URL or an api.tracker.gg profile URL.",
        )

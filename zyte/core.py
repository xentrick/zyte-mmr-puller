import base64
import binascii
import asyncio
import logging
import json
import time
from collections import defaultdict
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

import aiohttp
import requests
from pydantic import ValidationError
from zyte_api import (
    AggressiveRetryFactory,
    AsyncZyteAPI,
    ZyteAPI,
    stop_on_count,
    stop_on_download_error,
)

from zyte import devleague
from zyte.models import (
    CurrentMMRResult,
    PeakMMRByPlaylistResult,
    PeakMMRResult,
    PlaylistMMRRow,
    StandardProfile,
)
from zyte.segment_models import SegmentPlaylistResponse

log = logging.getLogger(__name__)


class TrackerSourcePullError(RuntimeError):
    """Raised when tracker source endpoints fail and execution must stop."""


# class TrackerGGAggressiveRetryFactory(AggressiveRetryFactory):
#     """Retry policy for tracker.gg responses that can be false negatives under load."""

#     throttling_stop = stop_on_count(5)
#     network_error_stop = stop_on_count(5)
#     download_error_stop = stop_on_download_error(max_total=5, max_permanent=5)
#     undocumented_error_stop = stop_on_count(5)


class ZyteMMRPuller:
    API_PROFILE_BASE_URL = (
        "https://api.tracker.gg/api/v2/rocket-league/standard/profile"
    )
    TRACKER_PROFILE_HOSTS = {
        "rocketleague.tracker.network",
        "www.rocketleague.tracker.network",
    }
    API_HOST = "api.tracker.gg"
    TRACKER_NEXT_URL = "https://api.rscna.com/api/v1/tracker-links/next/"
    DEVLEAGUE_GET_TRACKER_URL = "https://devleague.rscna.com/get_tracker?delete=True"
    DEVLEAGUE_SAVE_URL = devleague.DEVLEAGUE_SAVE_URL
    DEVLEAGUE_TARGET_PLAYLISTS = devleague.DEVLEAGUE_TARGET_PLAYLISTS

    def __init__(self, api_key: str):
        self.api_key = api_key
        # retrying = TrackerGGAggressiveRetryFactory().build()
        self.client = ZyteAPI(api_key=self.api_key)
        self.async_client = AsyncZyteAPI(api_key=self.api_key, n_conn=60)

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

    def get_current_mmr_by_season(
        self,
        url: str,
        season: int,
        http_response_body: bool = True,
    ) -> CurrentMMRResult:
        if season < 1:
            raise ValueError("season must be >= 1")

        payload = self.pull_season(
            url, season=season, http_response_body=http_response_body
        )
        if payload is None:
            return CurrentMMRResult(season=season, playlists=[])

        playlists: list[PlaylistMMRRow] = []
        for entry in payload.data:
            if entry.type != "playlist":
                continue

            rating = entry.stats.get("rating")
            matches = entry.stats.get("matchesPlayed")
            tier = entry.stats.get("tier")
            division = entry.stats.get("division")

            playlists.append(
                PlaylistMMRRow.model_validate(
                    {
                        "playlist_id": entry.attributes.playlistId,
                        "playlist_name": entry.metadata.name,
                        "games_played": matches.value if matches else 0,
                        "games_played_display": (
                            matches.displayValue if matches else None
                        ),
                        "rank_rating": rating.value if rating else 0,
                        "rank_rating_display": (
                            rating.displayValue if rating else None
                        ),
                        "rank_tier": tier.metadata.get("name") if tier else None,
                        "rank_tier_value": tier.value if tier else None,
                        "rank_division": (
                            division.metadata.get("name") if division else None
                        ),
                        "rank_division_value": division.value if division else None,
                    }
                )
            )

        playlists.sort(
            key=lambda row: (
                row.playlist_id is None,
                row.playlist_id,
                row.playlist_name,
            )
        )
        return CurrentMMRResult(season=season, playlists=playlists)

    async def get_current_mmr_by_season_async(
        self,
        url: str,
        season: int,
        http_response_body: bool = True,
    ) -> CurrentMMRResult:
        if season < 1:
            raise ValueError("season must be >= 1")

        payload = await self.pull_season_async(
            url,
            season=season,
            http_response_body=http_response_body,
        )
        if payload is None:
            return CurrentMMRResult(season=season, playlists=[])

        playlists: list[PlaylistMMRRow] = []
        for entry in payload.data:
            if entry.type != "playlist":
                continue

            rating = entry.stats.get("rating")
            matches = entry.stats.get("matchesPlayed")
            tier = entry.stats.get("tier")
            division = entry.stats.get("division")

            playlists.append(
                PlaylistMMRRow.model_validate(
                    {
                        "playlist_id": entry.attributes.playlistId,
                        "playlist_name": entry.metadata.name,
                        "games_played": matches.value if matches else 0,
                        "games_played_display": (
                            matches.displayValue if matches else None
                        ),
                        "rank_rating": rating.value if rating else 0,
                        "rank_rating_display": (
                            rating.displayValue if rating else None
                        ),
                        "rank_tier": tier.metadata.get("name") if tier else None,
                        "rank_tier_value": tier.value if tier else None,
                        "rank_division": (
                            division.metadata.get("name") if division else None
                        ),
                        "rank_division_value": division.value if division else None,
                    }
                )
            )

        playlists.sort(
            key=lambda row: (
                row.playlist_id is None,
                row.playlist_id,
                row.playlist_name,
            )
        )
        return CurrentMMRResult(season=season, playlists=playlists)

    def get_current_mmr_by_window(
        self,
        url: str,
        start_season: int,
        seasons_to_scan: int = 5,
        http_response_body: bool = True,
    ) -> CurrentMMRResult:
        if start_season < 1:
            raise ValueError("start_season must be >= 1")
        if seasons_to_scan < 1:
            raise ValueError("seasons_to_scan must be >= 1")

        min_season = max(1, start_season - seasons_to_scan + 1)
        fallback = CurrentMMRResult(season=start_season, playlists=[])

        for season in range(start_season, min_season - 1, -1):
            current = self.get_current_mmr_by_season(
                url,
                season=season,
                http_response_body=http_response_body,
            )
            if current.playlists:
                return current

        return fallback

    async def get_current_mmr_by_window_async(
        self,
        url: str,
        start_season: int,
        seasons_to_scan: int = 5,
        http_response_body: bool = True,
    ) -> CurrentMMRResult:
        if start_season < 1:
            raise ValueError("start_season must be >= 1")
        if seasons_to_scan < 1:
            raise ValueError("seasons_to_scan must be >= 1")

        min_season = max(1, start_season - seasons_to_scan + 1)
        fallback = CurrentMMRResult(season=start_season, playlists=[])

        for season in range(start_season, min_season - 1, -1):
            current = await self.get_current_mmr_by_season_async(
                url,
                season=season,
                http_response_body=http_response_body,
            )
            if current.playlists:
                return current

        return fallback

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
                    "games_played": matches.value if matches else 0,
                    "games_played_display": matches.displayValue if matches else 0,
                    "rank_rating": rating.value if rating else 0,
                    "rank_rating_display": rating.displayValue if rating else 0,
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
            matches = segment.stats.get("matchesPlayed")

            grouped[season].append(
                {
                    "playlist_id": playlist_id,
                    "playlist_name": segment.metadata.name,
                    "games_played": matches.value if matches else None,
                    "games_played_display": (matches.displayValue if matches else None),
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
    def _accumulate_season_payload_peaks(
        payload: SegmentPlaylistResponse,
        season: int,
        best_by_playlist: dict[tuple[int | None, str], PeakMMRResult],
    ) -> None:
        for entry in payload.data:
            if entry.type != "playlist":
                continue

            stats = entry.stats
            peak = stats.get("peakRating") or stats.get("rating")
            if peak is None or not isinstance(peak.value, (int, float)):
                continue

            matches = stats.get("matchesPlayed")
            peak_tier = stats.get("peakTier")
            peak_division = stats.get("peakDivision")
            tier = stats.get("tier")
            division = stats.get("division")

            candidate = PeakMMRResult.model_validate(
                {
                    "season": season,
                    "playlist_id": entry.attributes.playlistId,
                    "playlist_name": entry.metadata.name,
                    "games_played": matches.value if matches else None,
                    "games_played_display": (matches.displayValue if matches else None),
                    "rank_rating": peak.value,
                    "rank_rating_display": peak.displayValue,
                    "rank_tier": (
                        (peak_tier.metadata.get("name") if peak_tier else None)
                        or (tier.metadata.get("name") if tier else None)
                    ),
                    "rank_tier_value": (
                        peak_tier.value
                        if peak_tier is not None
                        else (tier.value if tier is not None else None)
                    ),
                    "rank_division": (
                        (peak_division.metadata.get("name") if peak_division else None)
                        or (
                            division.metadata.get("name")
                            if division is not None
                            else None
                        )
                    ),
                    "rank_division_value": (
                        peak_division.value
                        if peak_division is not None
                        else (division.value if division is not None else None)
                    ),
                }
            )

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
            candidate_rating = float(peak.value)

            if candidate_rating > current_rating:
                best_by_playlist[key] = candidate
            elif (
                candidate_rating == current_rating and candidate.season > current.season
            ):
                best_by_playlist[key] = candidate

    @staticmethod
    def _sorted_peak_result(
        best_by_playlist: dict[tuple[int | None, str], PeakMMRResult],
    ) -> PeakMMRByPlaylistResult:
        playlists = sorted(
            best_by_playlist.values(),
            key=lambda row: (
                row.playlist_id is None,
                row.playlist_id,
                row.playlist_name,
            ),
        )
        return PeakMMRByPlaylistResult(playlists=playlists)

    def get_peak_mmr_by_recent_seasons(
        self,
        url: str,
        start_season: int,
        seasons_to_scan: int = 5,
        http_response_body: bool = True,
    ) -> PeakMMRByPlaylistResult:
        if start_season < 1:
            raise ValueError("start_season must be >= 1")
        if seasons_to_scan < 1:
            raise ValueError("seasons_to_scan must be >= 1")

        min_season = max(1, start_season - seasons_to_scan + 1)
        best_by_playlist: dict[tuple[int | None, str], PeakMMRResult] = {}

        for season in range(start_season, min_season - 1, -1):
            payload = self.pull_season(
                url,
                season=season,
                http_response_body=http_response_body,
            )
            if payload is None:
                continue

            self._accumulate_season_payload_peaks(payload, season, best_by_playlist)

        return self._sorted_peak_result(best_by_playlist)

    async def get_peak_mmr_by_recent_seasons_async(
        self,
        url: str,
        start_season: int,
        seasons_to_scan: int = 5,
        http_response_body: bool = True,
    ) -> PeakMMRByPlaylistResult:
        if start_season < 1:
            raise ValueError("start_season must be >= 1")
        if seasons_to_scan < 1:
            raise ValueError("seasons_to_scan must be >= 1")

        min_season = max(1, start_season - seasons_to_scan + 1)
        best_by_playlist: dict[tuple[int | None, str], PeakMMRResult] = {}

        for season in range(start_season, min_season - 1, -1):
            payload = await self.pull_season_async(
                url,
                season=season,
                http_response_body=http_response_body,
            )
            if payload is None:
                continue

            self._accumulate_season_payload_peaks(payload, season, best_by_playlist)

        return self._sorted_peak_result(best_by_playlist)

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
    def _normalize_tracker_item(item: dict[str, Any]) -> dict[str, Any] | None:
        link = item.get("link")
        if not isinstance(link, str):
            for key in (
                "tracker_link",
                "trackerLink",
                "url",
                "tracker_url",
                "current_page",
                "currentPage",
            ):
                candidate = item.get(key)
                if isinstance(candidate, str):
                    link = candidate
                    break

        if not isinstance(link, str) or not link:
            return None

        normalized = dict(item)
        normalized["link"] = link
        return normalized

    @staticmethod
    def _extract_next_tracker_items(payload: Any) -> list[dict[str, Any]]:
        raw_items: list[dict[str, Any]] = []

        if isinstance(payload, list):
            raw_items = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            for key in ("results", "data", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    raw_items = [item for item in value if isinstance(item, dict)]
                    break

            if not raw_items:
                raw_items = [payload]

        normalized_items: list[dict[str, Any]] = []
        for item in raw_items:
            normalized = ZyteMMRPuller._normalize_tracker_item(item)
            if normalized is not None:
                normalized_items.append(normalized)

        return normalized_items

    @staticmethod
    def _extract_devleague_tracker_item(payload: Any) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None

        tracker = payload.get("tracker")
        if isinstance(tracker, dict):
            link = tracker.get("link")
            if isinstance(link, str) and link:
                normalized = dict(tracker)
                normalized["link"] = link
                return normalized

        link = payload.get("link")
        if isinstance(link, str) and link:
            normalized = dict(payload)
            normalized["link"] = link
            return normalized

        return None

    @staticmethod
    def pull_next_tracker_links(
        limit: int = 20,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        base_url = endpoint_url or ZyteMMRPuller.TRACKER_NEXT_URL
        parsed = urlparse(base_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["limit"] = str(limit)
        request_url = urlunparse(parsed._replace(query=urlencode(query)))

        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)

        try:
            response = requests.get(
                request_url,
                headers=request_headers,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            log.exception("Failed to pull tracker links from next endpoint")
            raise TrackerSourcePullError(
                f"Failed to pull tracker links from next endpoint: {exc}"
            ) from exc
        except ValueError as exc:
            log.exception("Failed to parse tracker links from next endpoint")
            raise TrackerSourcePullError(
                f"Failed to parse tracker links from next endpoint: {exc}"
            ) from exc

        return ZyteMMRPuller._extract_next_tracker_items(payload)

    @staticmethod
    async def pull_next_tracker_links_async(
        limit: int = 20,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        base_url = endpoint_url or ZyteMMRPuller.TRACKER_NEXT_URL
        parsed = urlparse(base_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["limit"] = str(limit)
        request_url = urlunparse(parsed._replace(query=urlencode(query)))

        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)

        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    request_url, headers=request_headers
                ) as response:
                    response.raise_for_status()
                    payload = await response.json(content_type=None)
                    log.debug(f"Next tracker payload: {payload}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.exception("Failed to pull tracker links from next endpoint")
            raise TrackerSourcePullError(
                f"Failed to pull tracker links from next endpoint: {exc}"
            ) from exc
        except ValueError as exc:
            log.exception("Failed to parse tracker links from next endpoint")
            raise TrackerSourcePullError(
                f"Failed to parse tracker links from next endpoint: {exc}"
            ) from exc

        return ZyteMMRPuller._extract_next_tracker_items(payload)

    @staticmethod
    def pull_devleague_tracker_links(
        limit: int = 20,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        request_url = endpoint_url or ZyteMMRPuller.DEVLEAGUE_GET_TRACKER_URL
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)

        trackers: list[dict[str, Any]] = []
        for _ in range(limit):
            try:
                response = requests.get(
                    request_url,
                    headers=request_headers,
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
            except requests.RequestException as exc:
                log.exception("Failed to pull tracker links from devleague endpoint")
                raise TrackerSourcePullError(
                    f"Failed to pull tracker links from devleague endpoint: {exc}"
                ) from exc
            except ValueError as exc:
                log.exception("Failed to parse tracker links from devleague endpoint")
                raise TrackerSourcePullError(
                    f"Failed to parse tracker links from devleague endpoint: {exc}"
                ) from exc

            tracker_item = ZyteMMRPuller._extract_devleague_tracker_item(payload)
            if tracker_item is None:
                break

            trackers.append(tracker_item)

            remaining = payload.get("remaining") if isinstance(payload, dict) else None
            if isinstance(remaining, int) and remaining <= 0:
                break

        return trackers

    @staticmethod
    async def pull_devleague_tracker_links_async(
        limit: int = 20,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        request_url = endpoint_url or ZyteMMRPuller.DEVLEAGUE_GET_TRACKER_URL
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)

        trackers: list[dict[str, Any]] = []
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for _ in range(limit):
                    async with session.get(
                        request_url,
                        headers=request_headers,
                    ) as response:
                        response.raise_for_status()
                        payload = await response.json(content_type=None)

                    tracker_item = ZyteMMRPuller._extract_devleague_tracker_item(
                        payload
                    )
                    if tracker_item is None:
                        break

                    trackers.append(tracker_item)

                    remaining = (
                        payload.get("remaining") if isinstance(payload, dict) else None
                    )
                    if isinstance(remaining, int) and remaining <= 0:
                        break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.exception("Failed to pull tracker links from devleague endpoint")
            raise TrackerSourcePullError(
                f"Failed to pull tracker links from devleague endpoint: {exc}"
            ) from exc
        except ValueError as exc:
            log.exception("Failed to parse tracker links from devleague endpoint")
            raise TrackerSourcePullError(
                f"Failed to parse tracker links from devleague endpoint: {exc}"
            ) from exc

        return trackers

    @staticmethod
    def iter_next_tracker_links(
        limit: int = 20,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
        poll_interval_seconds: float = 0.0,
        stop_on_empty: bool = True,
        max_batches: int | None = None,
    ):
        batches = 0
        while max_batches is None or batches < max_batches:
            links = ZyteMMRPuller.pull_next_tracker_links(
                limit=limit,
                endpoint_url=endpoint_url,
                timeout_seconds=timeout_seconds,
                headers=headers,
            )
            batches += 1

            if not links:
                if stop_on_empty:
                    return
                if poll_interval_seconds > 0:
                    time.sleep(poll_interval_seconds)
                continue

            for link in links:
                yield link

            if poll_interval_seconds > 0:
                time.sleep(poll_interval_seconds)

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

    @staticmethod
    def build_devleague_save_payload(
        profile: StandardProfile,
        tracker_link: str,
        pulled_by: str,
        notes: str = devleague.DEVLEAGUE_DEFAULT_NOTES,
        from_api: bool = False,
        status: str | None = None,
        date_pulled: str | None = None,
    ) -> dict[str, Any]:
        current = ZyteMMRPuller.get_current_mmr_latest_season(profile)
        peaks = ZyteMMRPuller.get_user_peak_mmr(profile)

        return devleague.build_devleague_save_payload(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link=tracker_link,
            pulled_by=pulled_by,
            notes=notes,
            from_api=from_api,
            status=status,
            date_pulled=date_pulled,
        )

    @staticmethod
    def post_devleague_payload(
        payload: dict[str, Any],
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> Any | None:
        return devleague.post_devleague_payload(
            payload=payload,
            endpoint_url=endpoint_url,
            timeout_seconds=timeout_seconds,
            headers=headers,
        )

    @staticmethod
    def post_profile_peaks_to_devleague(
        profile: StandardProfile,
        tracker_link: str,
        pulled_by: str,
        notes: str = devleague.DEVLEAGUE_DEFAULT_NOTES,
        from_api: bool = False,
        status: str | None = None,
        date_pulled: str | None = None,
        endpoint_url: str | None = None,
        timeout_seconds: float = 15.0,
        headers: dict[str, str] | None = None,
    ) -> Any | None:
        current = ZyteMMRPuller.get_current_mmr_latest_season(profile)
        peaks = ZyteMMRPuller.get_user_peak_mmr(profile)

        return devleague.post_profile_peaks_to_devleague(
            profile=profile,
            current=current,
            peaks=peaks,
            tracker_link=tracker_link,
            pulled_by=pulled_by,
            notes=notes,
            from_api=from_api,
            status=status,
            date_pulled=date_pulled,
            endpoint_url=endpoint_url,
            timeout_seconds=timeout_seconds,
            headers=headers,
        )

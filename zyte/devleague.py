import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp
import requests
from zyte.models import CurrentMMRResult, PeakMMRByPlaylistResult, StandardProfile

log = logging.getLogger(__name__)

DEVLEAGUE_SAVE_URL = "https://devleague.rscna.com/save_mmr?first=true"
DEVLEAGUE_BAD_TRACKER_URL = "https://devleague.rscna.com/bad_tracker"
DEVLEAGUE_DEFAULT_NOTES = "Automated pull by nickm"
DEVLEAGUE_TARGET_PLAYLISTS = {
    "Ranked Standard 3v3": "threes",
    "Ranked Doubles 2v2": "twos",
    "Ranked Duel 1v1": "ones",
}
DEVLEAGUE_PAYLOAD_KEY_ORDER = (
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
)


def _to_number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    return None


def _to_non_negative_mmr(value: Any) -> int | float:
    number = _to_number(value)
    if isinstance(number, (int, float)) and number > 0:
        return number
    return 0


def _utc_now_iso_millis() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def sort_devleague_payload(payload: dict[str, Any]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}

    for key in DEVLEAGUE_PAYLOAD_KEY_ORDER:
        if key in payload:
            ordered[key] = payload[key]

    for key in sorted(k for k in payload.keys() if k not in ordered):
        ordered[key] = payload[key]

    return ordered


def build_devleague_save_payload(
    profile: StandardProfile,
    current: CurrentMMRResult,
    peaks: PeakMMRByPlaylistResult,
    tracker_link: str,
    pulled_by: str,
    notes: str = DEVLEAGUE_DEFAULT_NOTES,
    from_api: bool = False,
    status: str | None = None,
    date_pulled: str | None = None,
) -> dict[str, Any]:
    current_by_name = {row.playlist_name: row for row in current.playlists}
    peaks_by_name = {row.playlist_name: row for row in peaks.playlists}

    payload: dict[str, Any] = {
        "from_api": from_api,
        "tracker_link": {"link": tracker_link},
    }

    for playlist_name, prefix in DEVLEAGUE_TARGET_PLAYLISTS.items():
        current_row = current_by_name.get(playlist_name)
        peak_row = peaks_by_name.get(playlist_name)

        payload[f"{prefix}_rating"] = (
            _to_non_negative_mmr(current_row.rank_rating)
            if current_row is not None
            else 0
        )
        payload[f"{prefix}_games_played"] = (
            _to_number(current_row.games_played) if current_row is not None else None
        )
        payload[f"{prefix}_season_peak"] = (
            _to_non_negative_mmr(peak_row.rank_rating) if peak_row is not None else 0
        )

    payload.update(
        {
            "notes": notes,
            "date_pulled": date_pulled or _utc_now_iso_millis(),
            "psyonix_season": current.season,
            "platform": profile.data.platformInfo.platformSlug,
            "user_id": profile.data.platformInfo.platformUserIdentifier,
            "pulled_by": pulled_by,
            "status": status,
        }
    )

    return sort_devleague_payload(payload)


def build_devleague_peak_season_payloads(
    profile: StandardProfile,
    current: CurrentMMRResult,
    peaks: PeakMMRByPlaylistResult,
    tracker_link: str,
    pulled_by: str,
    notes: str = DEVLEAGUE_DEFAULT_NOTES,
    from_api: bool = False,
    status: str | None = None,
    date_pulled: str | None = None,
) -> list[dict[str, Any]]:
    current_by_name = {row.playlist_name: row for row in current.playlists}
    peak_by_name = {
        row.playlist_name: row
        for row in peaks.playlists
        if row.playlist_name in DEVLEAGUE_TARGET_PLAYLISTS
    }

    seasons = sorted(
        {
            row.season
            for row in peak_by_name.values()
            if isinstance(row.season, int) and row.season > 0
        },
        reverse=True,
    )

    fallback_season = current.season if isinstance(current.season, int) else 0
    if not seasons:
        seasons = [fallback_season]

    payloads: list[dict[str, Any]] = []
    for season in seasons:
        payload: dict[str, Any] = {
            "from_api": from_api,
            "tracker_link": {"link": tracker_link},
        }

        for playlist_name, prefix in DEVLEAGUE_TARGET_PLAYLISTS.items():
            peak_row = peak_by_name.get(playlist_name)
            has_peak_this_season = bool(
                peak_row is not None
                and peak_row.season == season
                and isinstance(peak_row.rank_rating, (int, float))
            )
            peak_games_played = (
                _to_number(peak_row.games_played)
                if has_peak_this_season and peak_row is not None
                else 0
            )

            payload[f"{prefix}_season_peak"] = (
                _to_non_negative_mmr(peak_row.rank_rating)
                if has_peak_this_season
                else 0
            )

            # Only include current values when they align to the playlist peak season.
            if has_peak_this_season and current.season == season:
                current_row = current_by_name.get(playlist_name)
                payload[f"{prefix}_rating"] = (
                    _to_non_negative_mmr(current_row.rank_rating)
                    if current_row is not None
                    else 0
                )
                payload[f"{prefix}_games_played"] = (
                    _to_number(current_row.games_played)
                    if current_row is not None
                    else peak_games_played
                )
            else:
                payload[f"{prefix}_rating"] = 0
                payload[f"{prefix}_games_played"] = peak_games_played

        payload.update(
            {
                "notes": notes,
                "date_pulled": date_pulled or _utc_now_iso_millis(),
                "psyonix_season": season,
                "platform": profile.data.platformInfo.platformSlug,
                "user_id": profile.data.platformInfo.platformUserIdentifier,
                "pulled_by": pulled_by,
                "status": status,
            }
        )
        payloads.append(sort_devleague_payload(payload))

    return payloads


def build_bad_tracker_payload(
    pulled_by: str,
    tracker_link: str,
    current_page: str | None = None,
) -> dict[str, str]:
    current_href = current_page or tracker_link
    return {
        "pulled_by": pulled_by,
        "tracker_link": current_href,
        "current_page": current_href,
    }


def post_bad_tracker_payload(
    payload: dict[str, Any],
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    target_url = endpoint_url or DEVLEAGUE_BAD_TRACKER_URL
    request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if headers:
        request_headers.update(headers)

    try:
        response = requests.post(
            target_url,
            json=payload,
            headers=request_headers,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Failed to POST payload to devleague bad tracker endpoint")
        return None

    try:
        return response.json()
    except ValueError:
        return response.text


async def post_bad_tracker_payload_async(
    payload: dict[str, Any],
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    target_url = endpoint_url or DEVLEAGUE_BAD_TRACKER_URL
    request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if headers:
        request_headers.update(headers)

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                target_url,
                json=payload,
                headers=request_headers,
            ) as response:
                response.raise_for_status()
                raw = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        log.exception("Failed to POST payload to devleague bad tracker endpoint")
        return None

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def post_devleague_payload(
    payload: dict[str, Any],
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    target_url = endpoint_url or DEVLEAGUE_SAVE_URL
    request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if headers:
        request_headers.update(headers)

    try:
        response = requests.post(
            target_url,
            json=payload,
            headers=request_headers,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.Timeout as exc:
        log.warning("Timed out posting payload to devleague save endpoint: %s", exc)
        return {
            "timeout": True,
            "error": str(exc) or "Timeout posting payload to devleague save endpoint",
        }
    except requests.RequestException:
        log.exception("Failed to POST payload to devleague save endpoint")
        return None

    try:
        return response.json()
    except ValueError:
        return response.text


async def post_devleague_payload_async(
    payload: dict[str, Any],
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    target_url = endpoint_url or DEVLEAGUE_SAVE_URL
    request_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if headers:
        request_headers.update(headers)

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                target_url,
                json=payload,
                headers=request_headers,
            ) as response:
                response.raise_for_status()
                raw = await response.text()
    except asyncio.TimeoutError as exc:
        log.warning("Timed out posting payload to devleague save endpoint: %s", exc)
        return {
            "timeout": True,
            "error": str(exc) or "Timeout posting payload to devleague save endpoint",
        }
    except aiohttp.ClientError:
        log.exception("Failed to POST payload to devleague save endpoint")
        return None

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def post_profile_peaks_to_devleague(
    profile: StandardProfile,
    current: CurrentMMRResult,
    peaks: PeakMMRByPlaylistResult,
    tracker_link: str,
    pulled_by: str,
    notes: str = DEVLEAGUE_DEFAULT_NOTES,
    from_api: bool = False,
    status: str | None = None,
    date_pulled: str | None = None,
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    payload = build_devleague_save_payload(
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
    return post_devleague_payload(
        payload=payload,
        endpoint_url=endpoint_url,
        timeout_seconds=timeout_seconds,
        headers=headers,
    )


async def post_profile_peaks_to_devleague_async(
    profile: StandardProfile,
    current: CurrentMMRResult,
    peaks: PeakMMRByPlaylistResult,
    tracker_link: str,
    pulled_by: str,
    notes: str = DEVLEAGUE_DEFAULT_NOTES,
    from_api: bool = False,
    status: str | None = None,
    date_pulled: str | None = None,
    endpoint_url: str | None = None,
    timeout_seconds: float = 15.0,
    headers: dict[str, str] | None = None,
) -> Any | None:
    payload = build_devleague_save_payload(
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
    return await post_devleague_payload_async(
        payload=payload,
        endpoint_url=endpoint_url,
        timeout_seconds=timeout_seconds,
        headers=headers,
    )

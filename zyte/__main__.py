import argparse
import asyncio
import json
import logging
import os
from dotenv import load_dotenv

from zyte import devleague
from zyte.core import TrackerSourcePullError, ZyteMMRPuller
from zyte.models import CurrentMMRResult, PeakMMRByPlaylistResult
from zyte.segment_models import SegmentPlaylistResponse

load_dotenv()
log = logging.getLogger(__name__)


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pull MMR data from Zyte API")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_debug_arg(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debug logging",
        )

    def add_network_args(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument(
            "--async",
            dest="use_async",
            action="store_true",
            help="Use async Zyte client for network calls",
        )
        command_parser.add_argument(
            "--api-key",
            default=os.getenv("ZYTE_API_KEY"),
            help="Zyte API key (defaults to ZYTE_API_KEY env var)",
        )
        command_parser.add_argument(
            "--no-http-response-body",
            action="store_true",
            help="Disable httpResponseBody in Zyte API request options",
        )

    for command in ("current", "peaks", "playerid"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("url", help="Target URL to pull MMR data for")
        add_debug_arg(command_parser)
        add_network_args(command_parser)

    season_parser = subparsers.add_parser("season")
    season_parser.add_argument("url", help="Target URL to pull MMR data for")
    season_parser.add_argument("season", type=int, help="Season number to query")
    add_debug_arg(season_parser)
    add_network_args(season_parser)

    season_peaks_parser = subparsers.add_parser("season-peaks")
    season_peaks_parser.add_argument("url", help="Target URL to pull MMR data for")
    season_peaks_parser.add_argument(
        "season",
        type=int,
        help="Starting season number (inclusive)",
    )
    season_peaks_parser.add_argument(
        "--window",
        type=int,
        default=5,
        help="Number of seasons to scan including tharting season (default: 5)",
    )
    add_debug_arg(season_peaks_parser)
    add_network_args(season_peaks_parser)

    bulk_post_parser = subparsers.add_parser("post-next-peaks")
    bulk_post_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="How many tracker links to pull from tracker-links/next (default: 5)",
    )
    bulk_post_parser.add_argument(
        "--batches",
        type=int,
        default=1,
        help="How many batches to process; limit applies per batch (default: 1)",
    )
    bulk_post_parser.add_argument(
        "--pulled-by",
        default=os.getenv("DEVLEAGUE_PULLED_BY", "nickm"),
        help="Value for payload.pulled_by",
    )
    bulk_post_parser.add_argument(
        "--notes",
        default=devleague.DEVLEAGUE_DEFAULT_NOTES,
        help="Optional notes value for DevLeague payload",
    )
    bulk_post_parser.add_argument(
        "--status",
        default=None,
        help="Optional status value for DevLeague payload",
    )
    bulk_post_parser.add_argument(
        "--tracker-source",
        choices=("next", "devleague"),
        default="next",
        help="Tracker source endpoint: next uses tracker-links/next, devleague uses get_tracker",
    )
    bulk_post_parser.add_argument(
        "--tracker-next-url",
        default=ZyteMMRPuller.TRACKER_NEXT_URL,
        help="Override tracker-links/next endpoint URL when --tracker-source=next",
    )
    bulk_post_parser.add_argument(
        "--devleague-url",
        default=ZyteMMRPuller.DEVLEAGUE_SAVE_URL,
        help="Override DevLeague save endpoint URL",
    )
    bulk_post_parser.add_argument(
        "--from-api",
        action="store_true",
        help="Set payload.from_api to true",
    )
    bulk_post_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print DevLeague payloads without sending HTTP POST requests",
    )
    bulk_post_parser.add_argument(
        "--use-recent-season-peaks",
        action="store_true",
        help="Build peaks using the past 5-season pull system instead of profile lifetime peaks",
    )
    bulk_post_parser.add_argument(
        "--window",
        type=int,
        default=5,
        help="Number of seasons to scan when --use-recent-season-peaks is enabled (default: 5)",
    )
    bulk_post_parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Post data for a specific season number",
    )
    bulk_post_parser.add_argument(
        "--no-post-failed-trackers",
        action="store_true",
        help="Do not post failed tracker links to the DevLeague bad_tracker endpoint",
    )
    add_debug_arg(bulk_post_parser)
    add_network_args(bulk_post_parser)

    convert_parser = subparsers.add_parser("convert")
    convert_parser.add_argument("link", help="Tracker profile link to convert")
    add_debug_arg(convert_parser)

    return parser


def _build_bad_tracker_result_fields(
    bad_tracker_result: dict[str, object] | object | None,
) -> dict[str, object]:
    if (
        isinstance(bad_tracker_result, dict)
        and bad_tracker_result.get("disabled") is True
    ):
        return {
            "bad_tracker_reported": False,
            "bad_tracker_payload_preview": bad_tracker_result.get("payload"),
        }

    if (
        isinstance(bad_tracker_result, dict)
        and bad_tracker_result.get("dry_run") is True
    ):
        return {
            "bad_tracker_reported": False,
            "bad_tracker_payload_preview": bad_tracker_result.get("payload"),
        }

    return {
        "bad_tracker_reported": bad_tracker_result is not None,
        "bad_tracker_response": bad_tracker_result,
    }


def _is_devleague_post_timeout(result: object) -> bool:
    return isinstance(result, dict) and result.get("timeout") is True


def _report_bad_tracker_sync(
    args: argparse.Namespace,
    tracker_link: str,
) -> dict[str, object] | object | None:
    if args.no_post_failed_trackers:
        return {
            "disabled": True,
            "payload": devleague.build_bad_tracker_payload(
                pulled_by=args.pulled_by,
                tracker_link=tracker_link,
            ),
        }

    if args.dry_run:
        return {
            "dry_run": True,
            "payload": devleague.build_bad_tracker_payload(
                pulled_by=args.pulled_by,
                tracker_link=tracker_link,
            ),
        }

    payload = devleague.build_bad_tracker_payload(
        pulled_by=args.pulled_by,
        tracker_link=tracker_link,
    )
    return devleague.post_bad_tracker_payload(payload=payload)


async def _report_bad_tracker_async(
    args: argparse.Namespace,
    tracker_link: str,
) -> dict[str, object] | object | None:
    if args.no_post_failed_trackers:
        return {
            "disabled": True,
            "payload": devleague.build_bad_tracker_payload(
                pulled_by=args.pulled_by,
                tracker_link=tracker_link,
            ),
        }

    if args.dry_run:
        return {
            "dry_run": True,
            "payload": devleague.build_bad_tracker_payload(
                pulled_by=args.pulled_by,
                tracker_link=tracker_link,
            ),
        }

    payload = devleague.build_bad_tracker_payload(
        pulled_by=args.pulled_by,
        tracker_link=tracker_link,
    )
    return await devleague.post_bad_tracker_payload_async(payload=payload)


def _resolve_peaks_sync(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
    response: object,
    normalized_url: str,
    current: CurrentMMRResult,
) -> PeakMMRByPlaylistResult:
    if isinstance(args.season, int):
        return client.get_peak_mmr_by_recent_seasons(
            normalized_url,
            start_season=args.season,
            seasons_to_scan=1,
            http_response_body=not args.no_http_response_body,
        )
    if args.use_recent_season_peaks and isinstance(current.season, int):
        return client.get_peak_mmr_by_recent_seasons(
            normalized_url,
            start_season=current.season,
            seasons_to_scan=args.window,
            http_response_body=not args.no_http_response_body,
        )
    return ZyteMMRPuller.get_user_peak_mmr(response)


async def _resolve_peaks_async(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
    response: object,
    normalized_url: str,
    current: CurrentMMRResult,
) -> PeakMMRByPlaylistResult:
    if isinstance(args.season, int):
        return await client.get_peak_mmr_by_recent_seasons_async(
            normalized_url,
            start_season=args.season,
            seasons_to_scan=1,
            http_response_body=not args.no_http_response_body,
        )
    if args.use_recent_season_peaks and isinstance(current.season, int):
        return await client.get_peak_mmr_by_recent_seasons_async(
            normalized_url,
            start_season=current.season,
            seasons_to_scan=args.window,
            http_response_body=not args.no_http_response_body,
        )
    return ZyteMMRPuller.get_user_peak_mmr(response)


def _process_tracker_sync(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
    item: dict[str, object],
) -> dict[str, object]:
    tracker_link = item.get("link")
    if not isinstance(tracker_link, str):
        return {
            "tracker_link": None,
            "status": "skipped",
            "error": "Missing or invalid link in tracker item",
        }

    normalized_url, error = ZyteMMRPuller.normalize_cli_profile_url(tracker_link)
    if error is not None or normalized_url is None:
        bad_tracker_result = _report_bad_tracker_sync(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "status": "failed",
            "error": error or "Failed to normalize tracker link",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    try:
        response = client.pull_mmr(
            normalized_url,
            http_response_body=not args.no_http_response_body,
        )
    except Exception as exc:
        log.warning("Failed tracker due profile pull error: %s", exc)
        bad_tracker_result = _report_bad_tracker_sync(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": f"Profile pull error: {exc}",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    if response is None:
        bad_tracker_result = _report_bad_tracker_sync(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Profile not found or returned invalid payload",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    try:
        if isinstance(args.season, int):
            current = client.get_current_mmr_by_season(
                normalized_url,
                season=args.season,
                http_response_body=not args.no_http_response_body,
            )
        elif args.use_recent_season_peaks:
            latest = ZyteMMRPuller.get_current_mmr_latest_season(response)
            start_season = latest.season if isinstance(latest.season, int) else 1
            current = client.get_current_mmr_by_window(
                normalized_url,
                start_season=start_season,
                seasons_to_scan=args.window,
                http_response_body=not args.no_http_response_body,
            )
        else:
            current = ZyteMMRPuller.get_current_mmr_latest_season(response)

        peaks = _resolve_peaks_sync(client, args, response, normalized_url, current)
    except Exception as exc:
        log.warning("Failed tracker due season/peak pull error: %s", exc)
        bad_tracker_result = _report_bad_tracker_sync(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": f"Peak pull error: {exc}",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    payloads = devleague.build_devleague_peak_season_payloads(
        profile=response,
        current=current,
        peaks=peaks,
        tracker_link=tracker_link,
        pulled_by=args.pulled_by,
        notes=args.notes,
        from_api=args.from_api,
        status=args.status,
    )

    if args.dry_run:
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "dry-run",
            "devleague_payloads": payloads,
        }

    post_results = [
        devleague.post_devleague_payload(
            payload=payload,
            endpoint_url=args.devleague_url,
        )
        for payload in payloads
    ]

    if any(_is_devleague_post_timeout(result) for result in post_results):
        log.warning(
            "Timed out posting one or more DevLeague payloads for tracker: %s",
            tracker_link,
        )
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Timed out posting payload to DevLeague",
            "post_timeout": True,
            "devleague_payloads": payloads,
            "devleague_responses": post_results,
        }

    if any(result is None for result in post_results):
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Failed to post payload to DevLeague",
            "devleague_payloads": payloads,
            "devleague_responses": post_results,
        }

    return {
        "tracker_link": tracker_link,
        "profile_url": normalized_url,
        "status": "posted",
        "devleague_payloads": payloads,
        "devleague_responses": post_results,
    }


async def _process_tracker_async(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
    item: dict[str, object],
) -> dict[str, object]:
    log.debug("Processing tracker item: %s", item)
    tracker_link = item.get("link")
    if not isinstance(tracker_link, str):
        return {
            "tracker_link": None,
            "status": "skipped",
            "error": "Missing or invalid link in tracker item",
        }

    normalized_url, error = ZyteMMRPuller.normalize_cli_profile_url(tracker_link)
    if error is not None or normalized_url is None:
        bad_tracker_result = await _report_bad_tracker_async(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "status": "failed",
            "error": error or "Failed to normalize tracker link",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    try:
        log.debug("Pulling profile for normalized URL: %s", normalized_url)
        response = await client.pull_mmr_async(
            normalized_url,
            http_response_body=not args.no_http_response_body,
        )
    except Exception as exc:
        log.warning("Failed tracker due profile pull error: %s", exc)
        bad_tracker_result = await _report_bad_tracker_async(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": f"Profile pull error: {exc}",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    if response is None:
        bad_tracker_result = await _report_bad_tracker_async(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Profile not found or returned invalid payload",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    try:
        if isinstance(args.season, int):
            log.debug(
                "Getting current MMR for season %s for normalized URL: %s",
                args.season,
                normalized_url,
            )
            current = await client.get_current_mmr_by_season_async(
                normalized_url,
                season=args.season,
                http_response_body=not args.no_http_response_body,
            )
        elif args.use_recent_season_peaks:
            log.debug(
                "Getting current MMR for recent season peaks for normalized URL: %s",
                normalized_url,
            )
            latest = ZyteMMRPuller.get_current_mmr_latest_season(response)
            start_season = latest.season if isinstance(latest.season, int) else 1
            current = await client.get_current_mmr_by_window_async(
                normalized_url,
                start_season=start_season,
                seasons_to_scan=args.window,
                http_response_body=not args.no_http_response_body,
            )
        else:
            current = ZyteMMRPuller.get_current_mmr_latest_season(response)

        log.debug("Current MMR for normalized URL %s: %s", normalized_url, current)
        peaks = await _resolve_peaks_async(
            client,
            args,
            response,
            normalized_url,
            current,
        )
    except Exception as exc:
        log.warning("Failed tracker due season/peak pull error: %s", exc)
        bad_tracker_result = await _report_bad_tracker_async(args, tracker_link)
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": f"Peak pull error: {exc}",
            **_build_bad_tracker_result_fields(bad_tracker_result),
        }

    payloads = devleague.build_devleague_peak_season_payloads(
        profile=response,
        current=current,
        peaks=peaks,
        tracker_link=tracker_link,
        pulled_by=args.pulled_by,
        notes=args.notes,
        from_api=args.from_api,
        status=args.status,
    )

    if args.dry_run:
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "dry-run",
            "devleague_payloads": payloads,
        }

    post_results = await asyncio.gather(
        *(
            devleague.post_devleague_payload_async(
                payload=payload,
                endpoint_url=args.devleague_url,
            )
            for payload in payloads
        )
    )

    if any(_is_devleague_post_timeout(result) for result in post_results):
        log.warning(
            "Timed out posting one or more DevLeague payloads for tracker: %s",
            tracker_link,
        )
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Timed out posting payload to DevLeague",
            "post_timeout": True,
            "devleague_payloads": payloads,
            "devleague_responses": post_results,
        }

    if any(result is None for result in post_results):
        return {
            "tracker_link": tracker_link,
            "profile_url": normalized_url,
            "status": "failed",
            "error": "Failed to post payload to DevLeague",
            "devleague_payloads": payloads,
            "devleague_responses": post_results,
        }

    return {
        "tracker_link": tracker_link,
        "profile_url": normalized_url,
        "status": "posted",
        "devleague_payloads": payloads,
        "devleague_responses": post_results,
    }


async def _run_post_next_peaks_async(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
    trackers: list[dict[str, object]],
    batch_index: int,
    total_batches: int,
    processed_offset: int,
) -> list[dict[str, object]]:
    if not trackers:
        log.info(
            "post-next-peaks progress batch %s/%s: no trackers to process",
            batch_index,
            total_batches,
        )
        return []

    completed = 0
    lock = asyncio.Lock()

    async def run_item(item: dict[str, object]) -> dict[str, object]:
        nonlocal completed
        result = await _process_tracker_async(client, args, item)
        async with lock:
            completed += 1
            log.info(
                "post-next-peaks progress batch %s/%s tracker %s/%s (overall %s): status=%s",
                batch_index,
                total_batches,
                completed,
                len(trackers),
                processed_offset + completed,
                result.get("status"),
            )
        return result

    tasks = [asyncio.create_task(run_item(item)) for item in trackers]
    return await asyncio.gather(*tasks)


def _run_post_next_peaks_batches_sync(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
) -> tuple[list[dict[str, object]], int]:
    results: list[dict[str, object]] = []
    pulled_count = 0
    processed_count = 0

    for batch_index in range(1, args.batches + 1):
        log.info(
            "post-next-peaks progress batch %s/%s: pulling up to %s trackers",
            batch_index,
            args.batches,
            args.limit,
        )
        if args.tracker_source == "devleague":
            trackers = ZyteMMRPuller.pull_devleague_tracker_links(limit=args.limit)
        else:
            trackers = ZyteMMRPuller.pull_next_tracker_links(
                limit=args.limit,
                endpoint_url=args.tracker_next_url,
            )
        pulled_count += len(trackers)
        log.info(
            "post-next-peaks progress batch %s/%s: pulled %s trackers",
            batch_index,
            args.batches,
            len(trackers),
        )

        for tracker_index, item in enumerate(trackers, start=1):
            result = _process_tracker_sync(client, args, item)
            results.append(result)
            processed_count += 1
            log.info(
                "post-next-peaks progress batch %s/%s tracker %s/%s (overall %s): status=%s",
                batch_index,
                args.batches,
                tracker_index,
                len(trackers),
                processed_count,
                result.get("status"),
            )

    return results, pulled_count


async def _run_post_next_peaks_batches_async(
    client: ZyteMMRPuller,
    args: argparse.Namespace,
) -> tuple[list[dict[str, object]], int]:
    results: list[dict[str, object]] = []
    pulled_count = 0
    processed_count = 0

    for batch_index in range(1, args.batches + 1):
        log.info(
            "post-next-peaks progress batch %s/%s: pulling up to %s trackers",
            batch_index,
            args.batches,
            args.limit,
        )
        if args.tracker_source == "devleague":
            trackers = await ZyteMMRPuller.pull_devleague_tracker_links_async(
                limit=args.limit
            )
        else:
            trackers = await ZyteMMRPuller.pull_next_tracker_links_async(
                limit=args.limit,
                endpoint_url=args.tracker_next_url,
            )
        log.debug(f"Trackers: {trackers}")
        pulled_count += len(trackers)
        log.info(
            "post-next-peaks progress batch %s/%s: pulled %s trackers",
            batch_index,
            args.batches,
            len(trackers),
        )
        batch_results = await _run_post_next_peaks_async(
            client,
            args,
            trackers,
            batch_index=batch_index,
            total_batches=args.batches,
            processed_offset=processed_count,
        )
        results.extend(batch_results)
        processed_count += len(batch_results)

    return results, pulled_count


def main() -> None:
    args = build_parser().parse_args()
    configure_logging(getattr(args, "debug", False))

    if args.command == "convert":
        converted = ZyteMMRPuller.tracker_link_to_api_url(args.link)
        if converted is None:
            print("null")
            return

        print(converted)
        return

    if not args.api_key:
        log.error("Missing API key. Provide --api-key or set ZYTE_API_KEY.")
        raise SystemExit("Missing API key. Provide --api-key or set ZYTE_API_KEY.")

    client = ZyteMMRPuller(api_key=args.api_key)

    if args.command == "post-next-peaks":
        if args.limit < 1:
            raise SystemExit("--limit must be >= 1")
        if args.batches < 1:
            raise SystemExit("--batches must be >= 1")
        if args.window < 1:
            raise SystemExit("--window must be >= 1")
        if args.season is not None and args.season < 1:
            raise SystemExit("--season must be >= 1")

        log.info(
            "Starting post-next-peaks: source=%s batches=%s limit=%s async=%s dry_run=%s season=%s recent_season_peaks=%s",
            args.tracker_source,
            args.batches,
            args.limit,
            args.use_async,
            args.dry_run,
            args.season,
            args.use_recent_season_peaks,
        )

        try:
            if args.use_async:
                results, pulled_count = asyncio.run(
                    _run_post_next_peaks_batches_async(client, args)
                )
            else:
                results, pulled_count = _run_post_next_peaks_batches_sync(client, args)
        except TrackerSourcePullError as exc:
            raise SystemExit(str(exc)) from exc

        output = {
            "requested_limit": args.limit,
            "requested_batches": args.batches,
            "dry_run": args.dry_run,
            "pulled_count": pulled_count,
            "posted_count": sum(
                1 for result in results if result.get("status") == "posted"
            ),
            "dry_run_count": sum(
                1 for result in results if result.get("status") == "dry-run"
            ),
            "failed_count": sum(
                1 for result in results if result.get("status") == "failed"
            ),
            "skipped_count": sum(
                1 for result in results if result.get("status") == "skipped"
            ),
            "results": results,
        }
        log.info(
            "Finished post-next-peaks: pulled=%s posted=%s failed=%s dry_run=%s skipped=%s",
            output["pulled_count"],
            output["posted_count"],
            output["failed_count"],
            output["dry_run_count"],
            output["skipped_count"],
        )
        print(json.dumps(output, indent=2))
        return

    if not hasattr(args, "url"):
        raise SystemExit(
            f"Command requires URL handling not implemented: {args.command}"
        )

    normalized_url, error = ZyteMMRPuller.normalize_cli_profile_url(args.url)
    if error is not None:
        log.error(error)
        raise SystemExit(error)
    assert normalized_url is not None
    if normalized_url != args.url:
        log.info("Input URL normalized to API profile URL")
        log.debug("Normalized URL: %s", normalized_url)
    args.url = normalized_url

    log.debug("Running command '%s' for url=%s", args.command, args.url)
    if args.command == "season":
        log.debug("Pulling season payload for season=%s", args.season)
        if args.use_async:
            log.debug("Using async season pull")
            season_response = asyncio.run(
                client.pull_season_async(
                    args.url,
                    season=args.season,
                    http_response_body=not args.no_http_response_body,
                )
            )
        else:
            season_response = client.pull_season(
                args.url,
                season=args.season,
                http_response_body=not args.no_http_response_body,
            )
        if season_response is None:
            log.warning("No season response was returned")
            print("null")
            return

        print(
            json.dumps(
                season_response.model_dump(mode="json"), indent=2, sort_keys=True
            )
        )
        return

    if args.command == "season-peaks":
        log.debug(
            "Pulling season peaks from season=%s over window=%s",
            args.season,
            args.window,
        )
        if args.use_async:
            log.debug("Using async recent-season peaks pull")
            season_peaks = asyncio.run(
                client.get_peak_mmr_by_recent_seasons_async(
                    args.url,
                    start_season=args.season,
                    seasons_to_scan=args.window,
                    http_response_body=not args.no_http_response_body,
                )
            )
        else:
            season_peaks = client.get_peak_mmr_by_recent_seasons(
                args.url,
                start_season=args.season,
                seasons_to_scan=args.window,
                http_response_body=not args.no_http_response_body,
            )

        print(
            json.dumps(season_peaks.model_dump(mode="json"), indent=2, sort_keys=True)
        )
        return

    if args.use_async:
        log.debug("Using async profile pull")
        response = asyncio.run(
            client.pull_mmr_async(
                args.url,
                http_response_body=not args.no_http_response_body,
            )
        )
    else:
        response = client.pull_mmr(
            args.url,
            http_response_body=not args.no_http_response_body,
        )

    if response is None:
        log.warning("No profile response was returned")
        print("null")
        return

    parsed: (
        CurrentMMRResult | PeakMMRByPlaylistResult | SegmentPlaylistResponse | None
    ) = None
    if args.command == "current":
        log.debug("Parsing current MMR for latest season")
        parsed = ZyteMMRPuller.get_current_mmr_latest_season(response)
    elif args.command == "peaks":
        log.debug("Parsing peak MMR by playlist")
        parsed = ZyteMMRPuller.get_user_peak_mmr(response)
    elif args.command == "playerid":
        log.debug("Extracting playerId from profile metadata")
        print(ZyteMMRPuller.get_player_id(response))
        return
    else:
        raise SystemExit(f"Unknown command: {args.command}")

    if parsed is None:
        print("null")
        return

    print(json.dumps(parsed.model_dump(mode="json"), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

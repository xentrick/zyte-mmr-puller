import argparse
import asyncio
import json
import logging
import os
from dotenv import load_dotenv

from zyte.core import ZyteMMRPuller
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

    convert_parser = subparsers.add_parser("convert")
    convert_parser.add_argument("link", help="Tracker profile link to convert")
    add_debug_arg(convert_parser)

    return parser


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
    client = ZyteMMRPuller(api_key=args.api_key)
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

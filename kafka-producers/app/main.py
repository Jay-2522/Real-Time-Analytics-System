import argparse
import logging
from pathlib import Path

from app.adapters.cowrie import CowrieLogIngestor
from app.config.settings import get_settings
from app.producers.telemetry_producer import TelemetryProducer
from app.utils.logging import configure_logging
from app.utils.metrics import start_metrics_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka telemetry runtime")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    subparsers.add_parser("producer", help="run the synthetic telemetry producer")
    live = subparsers.add_parser("cowrie-live", help="follow new Cowrie JSON log lines")
    live.add_argument("--file", required=True, type=Path, help="path to cowrie.json")
    replay = subparsers.add_parser("cowrie-replay", help="publish a saved Cowrie JSON log once")
    replay.add_argument("--file", required=True, type=Path, help="path to saved Cowrie JSON")
    replay.add_argument(
        "--time-shift",
        action="store_true",
        help="move event times to now while preserving original relative gaps",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.producer_log_level)

    logger = logging.getLogger(__name__)
    start_metrics_server(settings.producer_metrics_port)
    logger.info("Starting Kafka telemetry runtime mode=%s", args.mode)

    if args.mode == "producer":
        TelemetryProducer(settings).run()
    elif args.mode == "cowrie-live":
        CowrieLogIngestor(settings).tail(args.file)
    else:
        CowrieLogIngestor(settings).replay(args.file, time_shift=args.time_shift)


if __name__ == "__main__":
    main()

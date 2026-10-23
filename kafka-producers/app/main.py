import argparse
import logging

from app.config.settings import get_settings
from app.producers.telemetry_producer import TelemetryProducer
from app.utils.logging import configure_logging
from app.utils.metrics import start_metrics_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka telemetry runtime")
    parser.add_argument("mode", choices=["producer"], help="Runtime mode to execute")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.producer_log_level)

    logger = logging.getLogger(__name__)
    start_metrics_server(settings.producer_metrics_port)
    logger.info("Starting Kafka telemetry runtime mode=%s", args.mode)

    TelemetryProducer(settings).run()


if __name__ == "__main__":
    main()

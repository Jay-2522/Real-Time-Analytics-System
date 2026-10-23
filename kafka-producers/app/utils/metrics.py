import logging

from prometheus_client import Counter, start_http_server

logger = logging.getLogger(__name__)

EVENTS_PUBLISHED_TOTAL = Counter("events_published_total", "Telemetry events published to Kafka.", ["topic", "event_type"])
PRODUCER_ERRORS_TOTAL = Counter("producer_errors_total", "Telemetry producer publish errors.", ["topic"])


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    logger.info("Prometheus metrics endpoint started port=%s", port)

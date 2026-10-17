from infrastructure_runtime.security import redis_options
import logging

import redis
from prometheus_client import start_http_server

from app.config.settings import get_settings
from app.consumers.threat_consumer import ThreatIntelConsumer
from app.db.session import SessionLocal, initialize_database
from app.detections.rule_engine import DetectionRuleEngine
from app.elastic.indexer import ThreatIndexer
from app.enrichment.ioc_enricher import IOCEnricher
from app.repositories.threat_repository import ThreatRepository
from app.scoring.threat_scorer import ThreatScorer
from app.utils.logging import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    start_http_server(settings.metrics_port)
    logger.info("threat_metrics_started port=%s", settings.metrics_port)
    initialize_database()
    try:
        redis_client = redis.Redis.from_url(settings.redis_url, socket_connect_timeout=1, socket_timeout=1, **redis_options())
        redis_client.ping()
    except Exception:
        redis_client = None
        logger.warning("threat_redis_unavailable continuing_without_cache=true")
    consumer = ThreatIntelConsumer(
        settings=settings,
        session_factory=SessionLocal,
        enricher=IOCEnricher(redis_client),
        rules=DetectionRuleEngine(settings.rules_path),
        scorer=ThreatScorer(),
        repository=ThreatRepository(),
        indexer=ThreatIndexer(settings.elasticsearch_host),
    )
    consumer.run()


if __name__ == "__main__":
    main()

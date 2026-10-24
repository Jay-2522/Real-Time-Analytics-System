from infrastructure_runtime.security import hardened, redis_options
import logging

import redis

from app.config.settings import AlertEngineSettings
from app.schemas.alerts import AlertEvent

logger = logging.getLogger(__name__)


class AlertDeduplicator:
    def __init__(self, settings: AlertEngineSettings) -> None:
        self._settings = settings
        self._client = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True, socket_connect_timeout=3, socket_timeout=3, ssl=hardened(), **redis_options())
        self._available = False

    def initialize(self) -> None:
        try:
            self._client.ping()
            self._available = True
            logger.info("redis_dedup_connected")
        except Exception as exc:
            self._available = False
            logger.warning("redis_dedup_unavailable action=no_cooldown reason=%s", exc)

    def update_live_caches(self, alert: AlertEvent, expires_at=None) -> None:
        from pipeline_runtime.cache import cache_once
        cache_once(self._client, "alert", alert.alert_id, f"latest:alert:{alert.entity_id}",
                   alert.timestamp.isoformat(), alert.model_dump(mode="json"),
                   [f"counters:alerts:severity:{alert.severity}", f"counters:alerts:entity:{alert.entity_id}"],
                   list_key="live:critical-alerts" if alert.severity == "critical" else "", expires_at=expires_at)

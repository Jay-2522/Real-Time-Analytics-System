import logging

import redis

from app.config.settings import AlertEngineSettings
from app.schemas.alerts import AlertEvent
from app.utils.metrics import ALERT_DEDUPLICATIONS_TOTAL

logger = logging.getLogger(__name__)


class AlertDeduplicator:
    def __init__(self, settings: AlertEngineSettings) -> None:
        self._settings = settings
        self._client = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True, socket_connect_timeout=3, socket_timeout=3)
        self._available = False

    def initialize(self) -> None:
        try:
            self._client.ping()
            self._available = True
            logger.info("redis_dedup_connected")
        except Exception as exc:
            self._available = False
            logger.warning("redis_dedup_unavailable action=no_cooldown reason=%s", exc)

    def should_emit(self, alert: AlertEvent) -> bool:
        if not self._available:
            return True
        key = f"alert:dedup:{alert.correlation_id}:{alert.severity}"
        try:
            count = self._client.incr(key)
            if count == 1:
                self._client.expire(key, self._settings.dedup_cooldown_seconds)
                return True
            ALERT_DEDUPLICATIONS_TOTAL.inc()
            logger.info("alert_deduplicated key=%s count=%s", key, count)
            return False
        except Exception as exc:
            logger.warning("redis_dedup_failed action=emit_alert reason=%s", exc)
            return True

    def update_live_caches(self, alert: AlertEvent) -> None:
        from pipeline_runtime.cache import cache_once
        cache_once(self._client, "alert", alert.alert_id, f"latest:alert:{alert.entity_id}",
                   alert.timestamp.isoformat(), alert.model_dump(mode="json"),
                   [f"counters:alerts:severity:{alert.severity}", f"counters:alerts:entity:{alert.entity_id}"],
                   list_key="live:critical-alerts" if alert.severity == "critical" else "")

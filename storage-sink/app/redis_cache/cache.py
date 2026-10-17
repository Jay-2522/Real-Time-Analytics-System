from infrastructure_runtime.security import hardened, redis_options
import logging

import redis

from app.config.settings import StorageSinkSettings
from app.schemas.records import StorageRecord
from app.utils.metrics import REDIS_CACHE_ERRORS_TOTAL

logger = logging.getLogger(__name__)


class RedisCache:
    def __init__(self, settings: StorageSinkSettings) -> None:
        self._client = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True, socket_connect_timeout=3, socket_timeout=3, ssl=hardened(), **redis_options())
        self._available = False

    def initialize(self) -> None:
        try:
            self._client.ping()
            self._available = True
            logger.info("redis_connected")
        except Exception as exc:
            self._available = False
            logger.warning("redis_unavailable action=continue_without_cache reason=%s", exc)

    def update(self, record: StorageRecord, key: str) -> None:
        from pipeline_runtime.cache import cache_once
        counters = []
        if record.event_type:
            counters.append(f"counters:events:{record.event_type}")
        if record.severity:
            counters.append(f"counters:severity:{record.severity}")
        latest = (f"latest:prediction:{record.entity_id}" if record.record_kind == "prediction"
                  else f"recent:high-risk:{record.entity_id or key}" if (record.ml_risk_score or record.risk_score or 0) >= 70
                  else f"latest:event:{record.entity_id or key}")
        try:
            cache_once(self._client, "storage", key, latest, record.timestamp_or_created.isoformat(), record.raw_payload, counters)
            if record.record_kind == "prediction" and (record.ml_risk_score or record.risk_score or 0) >= 70:
                cache_once(self._client, "storage-high-risk", key, f"recent:high-risk:{record.entity_id or key}",
                           record.timestamp_or_created.isoformat(), record.raw_payload, [])
        except Exception:
            REDIS_CACHE_ERRORS_TOTAL.inc()
            raise

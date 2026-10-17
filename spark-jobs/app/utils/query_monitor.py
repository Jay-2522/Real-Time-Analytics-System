import logging
import threading
from typing import List

from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class StreamingQueryMonitor:
    def __init__(self, queries: List[StreamingQuery], interval_seconds: int = 30) -> None:
        self._queries = queries
        self._interval_seconds = interval_seconds
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, name="streaming-query-monitor", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stopped.wait(self._interval_seconds):
            for query in self._queries:
                if not query.isActive:
                    logger.error("query_inactive name=%s exception=%s", query.name, query.exception())
                    continue
                progress = query.lastProgress
                if not progress:
                    logger.info("query_progress name=%s status=waiting_for_first_batch", query.name)
                    continue
                logger.info(
                    "query_progress name=%s batch_id=%s input_rows=%s processed_rows_per_second=%s",
                    query.name,
                    progress.get("batchId"),
                    progress.get("numInputRows"),
                    progress.get("processedRowsPerSecond"),
                )

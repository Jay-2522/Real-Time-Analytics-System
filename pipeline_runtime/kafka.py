"""One synchronous in-flight record; fail closed and replay after a bounded restart delay."""
import logging
import time
from prometheus_client import Counter

logger = logging.getLogger(__name__)
FAILURES = Counter("pipeline_processing_failures_total", "Unacknowledged processing/commit failures", ["service"])
COMMITS = Counter("pipeline_commits_total", "Confirmed input offset commits", ["service"])
REBALANCES = Counter("pipeline_rebalances_total", "Assignment/revocation/loss callbacks", ["service", "event"])

CONSUMER_OPTIONS = {"enable.auto.commit": False, "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000}


class DeliveryLoop:
    def __init__(self, consumer, service):
        self.consumer = consumer
        self.service = service
        self.epoch = 0

    def _rebalance(self, event):
        def callback(consumer, partitions):
            self.epoch += 1
            REBALANCES.labels(self.service, event).inc()
            logger.info("consumer_%s service=%s partitions=%s epoch=%s", event, self.service, partitions, self.epoch)
            # No offsets are stored implicitly and no callback commits in-flight work.
        return callback

    def subscribe(self, topics):
        self.consumer.subscribe(list(topics), on_assign=self._rebalance("assigned"),
                                on_revoke=self._rebalance("revoked"), on_lost=self._rebalance("lost"))

    def process(self, message, handler):
        epoch = self.epoch
        try:
            handler(message)
            if epoch != self.epoch:
                raise RuntimeError("Partition ownership changed before acknowledgement")
            results = self.consumer.commit(message=message, asynchronous=False)
            if results and any(p.error for p in results):
                raise RuntimeError(f"Offset commit failed: {results}")
            COMMITS.labels(self.service).inc()
        except Exception:
            FAILURES.labels(self.service).inc()
            logger.exception("input_unacknowledged service=%s topic=%s partition=%s offset=%s",
                             self.service, message.topic(), message.partition(), message.offset())
            raise

    def run(self, topics, handler, running):
        try:
            self.subscribe(topics)
            while running():
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                self.process(message, handler)
        except Exception:
            # Stop this process; never poll/commit a later record over a failed one.
            # Compose restarts it with unchanged group offsets. Local callers rerun it.
            time.sleep(5)
            raise
        finally:
            self.consumer.close()  # auto commit is disabled, including on close.


def publish_confirmed(producer, *, topic, key, value, timeout=15):
    outcomes = []
    producer.produce(topic=topic, key=key, value=value, callback=lambda error, message: outcomes.append(error))
    remaining = producer.flush(timeout)
    if remaining or len(outcomes) != 1 or outcomes[0] is not None:
        raise RuntimeError(f"Kafka delivery unconfirmed topic={topic} remaining={remaining} errors={outcomes}")

import logging
import signal
import threading

from pyspark.sql import DataFrame

from app.aggregations.window_metrics import build_risk_metrics, build_window_metrics
from app.config.settings import get_settings
from app.config.topics import (
    ANOMALY_EVENTS,
    API_EVENTS,
    LOGIN_EVENTS,
    NETWORK_EVENTS,
)
from app.feature_engineering.features import build_feature_pipeline
from app.schemas.telemetry import anomaly_event_schema, api_event_schema, login_event_schema, network_event_schema
from app.sinks.kafka_sink import write_json_to_kafka
from app.streaming.kafka_reader import read_kafka_topic
from app.transformations.normalization import normalize_event, parse_kafka_json, select_enriched_columns
from app.utils.logging import configure_logging
from app.utils.query_monitor import StreamingQueryMonitor
from app.utils.spark import create_spark_session

logger = logging.getLogger(__name__)


def build_source_stream(spark, settings, topic: str, schema) -> DataFrame:
    kafka_df = read_kafka_topic(spark=spark, settings=settings, topic=topic)
    return normalize_event(parse_kafka_json(kafka_df, schema))


def main() -> None:
    settings = get_settings()
    configure_logging("INFO")
    from app.utils.checkpoints import inspect_checkpoint
    for name in ("analytics_enriched_events", "analytics_window_metrics", "analytics_risk_metrics", "analytics_feature_engineering_v2"):
        inspect_checkpoint(f"{settings.checkpoint_root}/{name}", stateful=name != "analytics_enriched_events")
    spark = create_spark_session(settings)

    logger.info(
        "starting_spark_streaming app_name=%s kafka_bootstrap_servers=%s checkpoint_root=%s",
        settings.app_name,
        settings.kafka_bootstrap_servers,
        settings.checkpoint_root,
    )

    login_events = build_source_stream(spark, settings, LOGIN_EVENTS, login_event_schema)
    api_events = build_source_stream(spark, settings, API_EVENTS, api_event_schema)
    network_events = build_source_stream(spark, settings, NETWORK_EVENTS, network_event_schema)
    anomaly_events = build_source_stream(spark, settings, ANOMALY_EVENTS, anomaly_event_schema)

    enriched_events = (
        select_enriched_columns(login_events)
        .unionByName(select_enriched_columns(api_events), allowMissingColumns=True)
        .unionByName(select_enriched_columns(network_events), allowMissingColumns=True)
        .unionByName(select_enriched_columns(anomaly_events), allowMissingColumns=True)
        .filter("risk_score >= 0 AND risk_score <= 100")
    )

    window_metrics = build_window_metrics(enriched_events, settings)
    risk_metrics = build_risk_metrics(enriched_events, settings)
    feature_pipeline = build_feature_pipeline(login_events, api_events, network_events, anomaly_events, settings)

    specifications = [
        (enriched_events, settings.enriched_events_topic, "analytics_enriched_events", "analytics_enriched_events", "event_id"),
        (window_metrics, settings.window_metrics_topic, "analytics_window_metrics", "analytics_window_metrics", None),
        (risk_metrics, settings.risk_metrics_topic, "analytics_risk_metrics", "analytics_risk_metrics", "entity_id"),
        (feature_pipeline, settings.feature_engineering_topic, "analytics_feature_engineering", "analytics_feature_engineering_v2", "entity_id"),
    ]
    stopping = threading.Event()
    def request_stop(signum, frame):
        logger.info("spark_shutdown_requested signal=%s", signum)
        stopping.set()
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    queries = []
    monitor = None
    try:
        for frame, topic, name, checkpoint, key in specifications:
            queries.append(write_json_to_kafka(frame, settings, topic, name, checkpoint, key))
        monitor = StreamingQueryMonitor(queries)
        monitor.start()
        while not stopping.is_set():
            if spark.streams.awaitAnyTermination(1):
                break
    finally:
        logger.info("stopping_spark_streaming_queries")
        if monitor:
            monitor.stop()
        try:
            for query in queries:
                if query.isActive:
                    query.stop()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()

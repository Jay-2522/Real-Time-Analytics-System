import logging

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

    queries = [
        write_json_to_kafka(
            df=enriched_events,
            settings=settings,
            topic=settings.enriched_events_topic,
            query_name="analytics_enriched_events",
            checkpoint_name="analytics_enriched_events",
            key_column="event_id",
        ),
        write_json_to_kafka(
            df=window_metrics,
            settings=settings,
            topic=settings.window_metrics_topic,
            query_name="analytics_window_metrics",
            checkpoint_name="analytics_window_metrics",
        ),
        write_json_to_kafka(
            df=risk_metrics,
            settings=settings,
            topic=settings.risk_metrics_topic,
            query_name="analytics_risk_metrics",
            checkpoint_name="analytics_risk_metrics",
            key_column="entity_id",
        ),
        write_json_to_kafka(
            df=feature_pipeline,
            settings=settings,
            topic=settings.feature_engineering_topic,
            query_name="analytics_feature_engineering",
            checkpoint_name="analytics_feature_engineering_v2",
            key_column="entity_id",
        ),
    ]

    monitor = StreamingQueryMonitor(queries)
    monitor.start()

    try:
        spark.streams.awaitAnyTermination()
    finally:
        logger.info("stopping_spark_streaming_queries")
        monitor.stop()
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

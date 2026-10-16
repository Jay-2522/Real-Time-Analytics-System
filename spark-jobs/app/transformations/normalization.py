from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def parse_kafka_json(kafka_df: DataFrame, schema: StructType) -> DataFrame:
    return (
        kafka_df.select(
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
        )
        .withColumn("payload", F.from_json(F.col("raw_payload"), schema))
        .filter(F.col("payload").isNotNull())
        .select("source_topic", "source_partition", "source_offset", "kafka_timestamp", "kafka_key", "payload.*")
    )


def normalize_event(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("risk_score", F.col("risk_score").cast("double"))
        .withColumn("risk_score_normalized", F.round(F.col("risk_score") / F.lit(100.0), 4))
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 85, F.lit("critical"))
            .when(F.col("risk_score") >= 70, F.lit("high"))
            .when(F.col("risk_score") >= 40, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .withColumn("processed_at", F.current_timestamp())
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
    )


def select_enriched_columns(df: DataFrame) -> DataFrame:
    expected_columns = {
        "event_id": None,
        "event_timestamp": None,
        "event_type": None,
        "risk_score": None,
        "risk_score_normalized": None,
        "risk_level": None,
        "source_ip": None,
        "user_id": None,
        "login_success": None,
        "destination_ip": None,
        "country": None,
        "endpoint": None,
        "status_code": None,
        "response_time_ms": None,
        "bytes_sent": None,
        "bytes_received": None,
        "anomaly_type": None,
        "severity": None,
        "source_topic": None,
        "source_partition": None,
        "source_offset": None,
        "processed_at": None,
    }

    selected = []
    for column_name in expected_columns:
        if column_name in df.columns:
            selected.append(F.col(column_name))
        else:
            selected.append(F.lit(None).alias(column_name))
    return df.select(*selected)

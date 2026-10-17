from infrastructure_runtime.security import kafka_java_options
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from typing import Optional
from app.utils.checkpoints import prepare_checkpoint, contract_signature

from app.config.settings import SparkStreamingSettings


def write_json_to_kafka(
    df: DataFrame,
    settings: SparkStreamingSettings,
    topic: str,
    query_name: str,
    checkpoint_name: str,
    key_column: Optional[str] = None,
    output_mode: str = "append",
) -> StreamingQuery:
    prepare_checkpoint(f"{settings.checkpoint_root}/{checkpoint_name}",
                       contract_signature(settings, topic, query_name, df.schema.json()),
                       stateful=query_name != "analytics_enriched_events")
    serializable_df = df.select(
        F.coalesce(F.col(key_column).cast("string"), F.expr("uuid()")).alias("key") if key_column else F.expr("uuid()").alias("key"),
        F.to_json(F.struct(*[F.col(column) for column in df.columns]), {"ignoreNullFields": "false", "timeZone": "UTC"}).alias("value"),
    )

    return (
        serializable_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .options(**kafka_java_options())
        .option("topic", topic)
        .option("checkpointLocation", f"{settings.checkpoint_root}/{checkpoint_name}")
        .queryName(query_name)
        .outputMode(output_mode)
        .trigger(processingTime=settings.trigger_processing_time)
        .start()
    )

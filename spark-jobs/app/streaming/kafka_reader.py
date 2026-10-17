from infrastructure_runtime.security import kafka_java_options
from pyspark.sql import DataFrame, SparkSession

from app.config.settings import SparkStreamingSettings


def read_kafka_topic(spark: SparkSession, settings: SparkStreamingSettings, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .options(**kafka_java_options())
        .option("subscribe", topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )

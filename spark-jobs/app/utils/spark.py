from pyspark.sql import SparkSession

from app.config.settings import SparkStreamingSettings


def create_spark_session(settings: SparkStreamingSettings) -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    return spark

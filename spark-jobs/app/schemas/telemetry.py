from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

COMMON_FIELDS = [
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("original_timestamp", StringType(), nullable=True),
    StructField("risk_score", DoubleType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
]

login_event_schema = StructType(
    COMMON_FIELDS
    + [
        StructField("user_id", StringType(), nullable=True),
        StructField("username", StringType(), nullable=True),
        StructField("source_ip", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("login_success", BooleanType(), nullable=True),
        StructField("failure_reason", StringType(), nullable=True),
    ]
)

api_event_schema = StructType(
    COMMON_FIELDS
    + [
        StructField("user_id", StringType(), nullable=True),
        StructField("endpoint", StringType(), nullable=True),
        StructField("method", StringType(), nullable=True),
        StructField("status_code", IntegerType(), nullable=True),
        StructField("response_time_ms", IntegerType(), nullable=True),
        StructField("source_ip", StringType(), nullable=True),
        StructField("request_size", IntegerType(), nullable=True),
    ]
)

network_event_schema = StructType(
    COMMON_FIELDS
    + [
        StructField("source_ip", StringType(), nullable=True),
        StructField("destination_ip", StringType(), nullable=True),
        StructField("protocol", StringType(), nullable=True),
        StructField("bytes_sent", IntegerType(), nullable=True),
        StructField("bytes_received", IntegerType(), nullable=True),
        StructField("port", IntegerType(), nullable=True),
        StructField("packet_count", IntegerType(), nullable=True),
    ]
)

anomaly_event_schema = StructType(
    COMMON_FIELDS
    + [
        StructField("anomaly_type", StringType(), nullable=True),
        StructField("severity", StringType(), nullable=True),
        StructField("source_ip", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
    ]
)

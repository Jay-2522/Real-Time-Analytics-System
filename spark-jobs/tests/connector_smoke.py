from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("offline-kafka-connector-smoke").getOrCreate()
try:
    spark._jvm.java.lang.Class.forName("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    spark._jvm.java.lang.Class.forName("org.apache.spark.sql.kafka010.KafkaWrite")
    print("PASS: Spark Kafka source and sink classes loaded without dependency resolution")
finally:
    spark.stop()

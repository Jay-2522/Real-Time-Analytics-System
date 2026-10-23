FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/jobs
ENV PYTHONPATH=/opt/spark/jobs

COPY spark-jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG SPARK_KAFKA_CONNECTOR_VERSION=3.5.3
RUN echo 'print("Kafka connector dependencies cached")' >/tmp/resolve_kafka.py \
    && /opt/spark/bin/spark-submit \
       --master 'local[1]' \
       --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_KAFKA_CONNECTOR_VERSION}" \
       /tmp/resolve_kafka.py \
    && cp /root/.ivy2/jars/*.jar /opt/spark/jars/ \
    && rm -rf /root/.ivy2 /tmp/resolve_kafka.py

COPY spark-jobs/app ./app
COPY infrastructure_runtime ./infrastructure_runtime

RUN mkdir -p /opt/spark/checkpoints /opt/spark/state

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://streaming-analytics-spark-master:7077", "app/main.py"]

FROM apache/spark:3.5.7 AS spark
FROM eclipse-temurin:17-jre AS java
FROM python:3.11-slim-bookworm
COPY --from=java /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark PATH=/opt/java/openjdk/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends procps bash && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/spark/jobs
ENV PYTHONPATH=/opt/spark/jobs PYTHONDONTWRITEBYTECODE=1
COPY spark-jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Resolve dependencies at build time; hardened runtime networks have no Internet egress.
RUN echo 'print("Kafka dependencies cached")' >/tmp/resolve.py && /opt/spark/bin/spark-submit --master 'local[1]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /tmp/resolve.py && cp /root/.ivy2/jars/*.jar /opt/spark/jars/ && rm -rf /root/.ivy2 /tmp/resolve.py
COPY spark-jobs/app ./app
COPY infrastructure_runtime ./infrastructure_runtime
RUN groupadd -g 185 spark && useradd -u 185 -g spark -m spark && mkdir -p /opt/spark/work /opt/spark/checkpoints && chown -R 185:185 /opt/spark/work /opt/spark/checkpoints
USER 185:185
CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "app/main.py"]

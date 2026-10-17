FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/jobs
ENV PYTHONPATH=/opt/spark/jobs

COPY spark-jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark-jobs/app ./app
COPY infrastructure_runtime ./infrastructure_runtime

RUN mkdir -p /opt/spark/checkpoints /opt/spark/state

CMD ["/opt/spark/bin/spark-submit", "--master", "spark://streaming-analytics-spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "app/main.py"]

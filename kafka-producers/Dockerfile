FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY kafka-producers/requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY kafka-producers/app ./app

CMD ["python", "-m", "app.main", "producer"]

COPY infrastructure_runtime /app/infrastructure_runtime
COPY pipeline_runtime /app/pipeline_runtime

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY ml-models/requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY ml-models/app ./app
COPY pipeline_runtime ./pipeline_runtime
RUN mkdir -p /app/artifacts

COPY infrastructure_runtime /app/infrastructure_runtime

CMD ["python", "-m", "app.main", "inference"]

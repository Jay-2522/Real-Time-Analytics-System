FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY alert-engine/requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY alert-engine/app ./app
COPY pipeline_runtime ./pipeline_runtime

CMD ["python", "-m", "app.main"]


FROM python:3.11-slim AS runtime
ARG SERVICE
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY ${SERVICE}/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && pip uninstall -y pip setuptools wheel && groupadd -g 10001 soc && useradd -u 10001 -g soc -M soc
COPY ${SERVICE}/app /app/app
COPY pipeline_runtime /app/pipeline_runtime
COPY infrastructure_runtime /app/infrastructure_runtime
USER 10001:10001
CMD ["python", "-m", "app.main"]
FROM runtime AS threat
COPY detections/rules /app/rules

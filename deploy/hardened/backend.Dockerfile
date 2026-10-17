FROM python:3.11-slim AS runtime
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && pip uninstall -y pip setuptools wheel && groupadd -g 10001 soc && useradd -u 10001 -g soc -M soc
COPY backend/app /app/app
COPY infrastructure_runtime /app/infrastructure_runtime
USER 10001:10001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "172.29.250.2", "--no-access-log"]



FROM runtime AS bootstrap
COPY deploy/hardened/bootstrap.py /setup/bootstrap.py
COPY migrations /setup/migrations
CMD ["python", "/setup/bootstrap.py"]

"""Small authenticated exporter; no event payloads or RPC credentials are exposed."""
import hmac
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class QueryMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = {}
        self.updated = 0.0
        self.server = None

    def update(self, queries):
        rows = {}
        for query in queries:
            progress = query.lastProgress or {}
            rows[query.name] = (int(query.isActive), progress.get('batchId', -1),
                                progress.get('numInputRows', 0))
        with self.lock:
            self.rows, self.updated = rows, time.time()

    def render(self):
        with self.lock:
            lines = [f'spark_query_monitor_timestamp_seconds {self.updated}']
            for name, (active, batch, rows) in self.rows.items():
                # Query names are repository-controlled; disallow metric-label injection.
                if not name.replace('_', '').isalnum():
                    continue
                for metric, value in [('active', active), ('batch_id', batch), ('input_rows', rows)]:
                    lines.append(f'spark_streaming_query_{metric}{{query="{name}"}} {value}')
            return ('\n'.join(lines) + '\n').encode()

    def start(self):
        path = os.environ.get('SPARK_METRICS_TOKEN_FILE')
        if not path:
            return  # Local contract tests have no published exporter.
        token = Path(path).read_text().strip()
        if not token:
            raise RuntimeError('Spark monitoring token is empty')
        metrics = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                    self.send_error(401)
                    return
                if self.path != '/metrics':
                    self.send_error(404)
                    return
                body = metrics.render()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; version=0.0.4')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('0.0.0.0', 9107), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()

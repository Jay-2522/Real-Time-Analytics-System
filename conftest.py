"""Isolate independently packaged services and their import-time metrics in tests."""
import os
import sys
import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def isolate_service_imports():
    collectors = set(REGISTRY._collector_to_names)
    path = sys.path[:]
    environ = os.environ.copy()
    yield
    # Remove only collectors registered by this test; duplicate registration still raises.
    for collector in set(REGISTRY._collector_to_names) - collectors:
        REGISTRY.unregister(collector)
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path[:] = path
    os.environ.clear()
    os.environ.update(environ)

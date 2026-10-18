"""Regression guards for formerly swallowed failures and false pipeline health."""
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT=Path(__file__).resolve().parents[2]


def load(service,module):
    for name in list(sys.modules):
        if name=='app' or name.startswith('app.'):
            del sys.modules[name]
    sys.path.insert(0,str(ROOT/service))
    return importlib.import_module(module)


def test_telemetry_deadletter_requires_confirmation():
    module=load('kafka-producers','app.consumers.telemetry_consumer')
    consumer=module.TelemetryConsumer(SimpleNamespace(deadletter_topic='dlq'))
    class FailedProducer:
        def produce(self,**kw):self.callback=kw['callback']
        def flush(self,timeout):self.callback(RuntimeError('broker down'),None);return 0
    consumer._deadletter_producer=FailedProducer()
    with pytest.raises(RuntimeError,match='unconfirmed'):
        consumer._handle_message('input',b'not-json',{'partition':0,'offset':5})
    payloads=[]
    class ConfirmedProducer:
        def produce(self,**kw):
            payloads.append(kw['value']);self.callback=kw['callback']
        def flush(self,timeout):self.callback(None,None);return 0
    consumer._deadletter_producer=ConfirmedProducer()
    for _ in range(2):
        consumer._handle_message('input',b'not-json',{'partition':0,'offset':5},1_700_000_000_000)
    assert payloads[0]==payloads[1], 'A replay must retain identity and timestamp'


def test_synthetic_producer_does_not_report_unconfirmed_publication():
    module=load('kafka-producers','app.producers.telemetry_producer')
    producer=module.TelemetryProducer(SimpleNamespace(anomaly_rate=.1))
    class TimedOutProducer:
        def produce(self,**kw):pass
        def flush(self,timeout):return 1
    producer._producer=TimedOutProducer()
    event=producer._generator.next_event()
    with pytest.raises(RuntimeError,match='unconfirmed'):producer._publish_event(event)


def test_threat_index_failure_propagates_and_retry_is_not_disabled():
    module=load('threat-intelligence-engine','app.elastic.indexer')
    indexer=module.ThreatIndexer.__new__(module.ThreatIndexer)
    calls=[]
    class Client:
        failed=True
        def index(self,**kw):
            calls.append(kw)
            if self.failed:raise ConnectionError('search down')
    indexer._client=Client()
    event=SimpleNamespace(event_id='stable-id',iocs=[],rule_hits=[],entity_id='entity',model_dump=lambda **kw:{'event_id':'stable-id'})
    with pytest.raises(ConnectionError):indexer.index(event)
    indexer._client.failed=False
    indexer.index(event)
    assert calls[0]['id']==calls[1]['id']=='stable-id:threat-intel-events'
    assert {x['index'] for x in calls}=={'threat-intel-events','entity-profiles','attack-timelines'}


@pytest.mark.parametrize('active,age,expected',[(None,None,'degraded'),(3,1,'degraded'),(4,90,'degraded'),(4,1,'healthy')])
def test_spark_health_requires_all_queries_and_fresh_observation(active,age,expected):
    module=load('backend','app.services.monitoring_service')
    service=module.MonitoringService(SimpleNamespace())
    service._prometheus_scalar=lambda query: active if query.startswith('sum(') else age
    assert service._spark_status({'spark-driver':1},service._now()).status==expected


def test_incident_projection_rejects_stale_snapshot_but_retries_transport_failure(monkeypatch):
    module=load('alert-engine','app.elastic.indexer')
    class OlderSnapshot(Exception):pass
    monkeypatch.setattr(module,'ConflictError',OlderSnapshot)
    calls=[]
    class Client:
        error=OlderSnapshot()
        def index(self,**kw):
            calls.append(kw)
            raise self.error
    indexer=module.AlertIndexer.__new__(module.AlertIndexer)
    indexer._client=Client()
    indexer.index_incident({'incident_id':'incident'},7)
    assert calls[0]['version']==7 and calls[0]['version_type']=='external_gte'
    indexer._client.error=ConnectionError('search unavailable')
    with pytest.raises(ConnectionError):indexer.index_incident({'incident_id':'incident'},8)

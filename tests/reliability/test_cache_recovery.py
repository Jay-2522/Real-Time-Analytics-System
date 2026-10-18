"""Permanent guards for deterministic SQL-derived cache values and expiry."""
import json
import sqlite3
import time

import pytest

from pipeline_runtime.cache_recovery import Projection, effects


def alert(identity, entity='fixture', severity='high', stamp='2026-10-18T00:00:00+00:00', expiry=100):
    return {'alert_id':identity, 'entity_id':entity, 'severity':severity, 'timestamp':stamp,
            '_cache_expires_at':expiry}


def projected(rows):
    db = sqlite3.connect(':memory:')
    p = Projection(db)
    for key, body in rows:
        p.add('alert', key, body)
    return db, p


def test_reconstruction_counts_exact_dimensions_and_order_independent_latest():
    rows = [('b',alert('b',severity='critical')),('a',alert('a')),('c',alert('c',entity='other'))]
    db,p = projected(rows)
    expected = dict((k,(v,e)) for k,v,e in p.values())
    assert expected['counters:alerts:entity:fixture'] == ('2',None)
    assert expected['counters:alerts:severity:high'] == ('2',None)
    assert expected['counters:alerts:severity:critical'] == ('1',None)
    assert json.loads(expected['latest:alert:fixture'][0])['alert_id'] == 'b'
    assert expected['latest:alert:fixture'][1] == 100
    assert '_cache_expires_at' not in json.loads(expected['latest:alert:fixture'][0])
    db2,p2 = projected(reversed(rows))
    assert dict((k,(v,e)) for k,v,e in p2.values()) == expected
    assert list(db.execute('SELECT value FROM critical')) == list(db2.execute('SELECT value FROM critical'))


def test_marker_retention_and_expired_latest_are_distinct():
    db,p = projected([('a',alert('a',expiry=time.time()-10))])
    result = {k:(v,e) for k,v,e in p.values()}
    assert result['pipeline:cache:alert:a'] == ('1',None)
    assert result['latest:alert:fixture'][1] < time.time()
    assert result['latest:alert:fixture:version'][1] is None
    assert result['counters:alerts:severity:high'][1] is None


def test_legacy_unknown_ttl_fails_instead_of_extending_stale_markers():
    body = alert('old');body.pop('_cache_expires_at')
    with pytest.raises(ValueError,match='durable expiry'):
        projected([('old',body)])


def test_storage_contract_rejects_missing_time_and_retains_high_risk_dimensions():
    body = {'record_kind':'prediction','timestamp':None,'raw_payload':{},'entity_id':'e',
            'ml_risk_score':90,'event_type':'prediction','severity':'high'}
    with pytest.raises(ValueError,match='durable timestamp'):
        effects('storage','k',body)
    body['timestamp']='2026-10-18T00:00:00+00:00'
    result = effects('storage','k',body)
    assert result[0][0:3] == ('storage','k','latest:prediction:e')
    assert result[0][5] == ['counters:events:prediction','counters:severity:high']
    assert result[1][0:3] == ('storage-high-risk','k','recent:high-risk:e')
    assert result[1][5] == []


def test_redis_ping_never_certifies_cache_correctness(monkeypatch):
    from tests.reliability.test_runtime_adapters import load
    from types import SimpleNamespace
    module=load('backend','app.services.monitoring_service')
    monkeypatch.setattr(module.redis.Redis,'from_url',lambda *a,**k:SimpleNamespace(ping=lambda:True))
    service=module.MonitoringService(SimpleNamespace(redis_url='redis://unused'))
    assert service._redis_status(service._now()).status == 'degraded'

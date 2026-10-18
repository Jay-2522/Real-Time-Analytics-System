"""SQL/outbox cache repair on marked, disposable TLS infrastructure."""
import os
import pytest
from test_stack import compose

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('HARDENED_CACHE_RECOVERY') != '1',
    reason='Requires marked disposable SQL/Redis stack and completed deterministic fixture')]


def test_completed_effects_partial_loss_interruption_concurrency_and_expiry():
    from pathlib import Path
    assert (Path(os.environ['HARDENED_SECRETS_DIR'])/'test-only').is_file()
    compose('exec','-T','alert-engine','python','-',timeout=180,input='''
import json,time,threading,uuid
from datetime import datetime,timezone
from sqlalchemy import select,func
from pipeline_runtime.cache_recovery import CacheReconciler,require_complete_ledger
from pipeline_runtime.durable import Outbox
from app.db.models import Alert
from app.main import (get_settings,create_session_factory,AlertProcessingService,AlertRulesEngine,
 AlertDeduplicator,IncidentCorrelator,AlertRepository,AlertIndexer,AlertEngineConsumer)
settings=get_settings();factory=create_session_factory(settings)
cache=AlertDeduplicator(settings);r=cache._client
service=AlertProcessingService(AlertRulesEngine(),cache,IncidentCorrelator(settings),AlertRepository(),AlertIndexer(settings))
consumer=AlertEngineConsumer(settings,factory,service)
recovery=CacheReconciler(factory,'alert',r)
recovery.run()
with factory() as db:
 rows=db.execute(select(Outbox.key,Outbox.payload,Outbox.done).where(Outbox.service=='alert',Outbox.target=='redis',Outbox.payload['entity_id'].astext=='192.0.2.181')).all()
assert len(rows)==44 and all(row.done for row in rows)
# A legacy business alert without its durable projection must fail coverage.
with factory() as db:
 original=db.scalars(select(Alert).where(Alert.entity_id=='192.0.2.181').limit(1)).one()
 values={c.name:getattr(original,c.name) for c in Alert.__table__.columns if c.name!='id'}
 db.add(Alert(**{**values,'alert_id':str(uuid.uuid4())}));db.flush()
 try:require_complete_ledger(db,'alert')
 except ValueError:pass
 else:raise AssertionError('Incomplete alert ledger accepted')
 finally:db.rollback()
counter='counters:alerts:entity:192.0.2.181'
assert int(r.get(counter))==44
# Partial restoration plus inconsistent counter and incorrect TTL. Completed SQL
# effects remain completed and are included in reconstruction.
for row in rows[:22]:r.delete('pipeline:cache:alert:'+row.payload['alert_id'])
r.lpush('pipeline:cache:alert:'+rows[0].payload['alert_id'],'invalid type')
r.set(counter,22,ex=10)
try:recovery.run(repair=False)
except ValueError:pass
else:raise AssertionError('Inconsistent cache accepted')
assert not r.exists('pipeline:cache:alert:verified')
class Interrupted:
 def __init__(self):self.n=0
 def __getattr__(self,name):return getattr(r,name)
 def set(self,*args,**kwargs):
  self.n+=1
  if self.n==10:raise ConnectionError('simulated process interruption')
  return r.set(*args,**kwargs)
try:CacheReconciler(factory,'alert',Interrupted()).run()
except ConnectionError:pass
else:raise AssertionError('Interrupted rebuild reported success')
assert not r.exists('pipeline:cache:alert:verified')
for _ in range(2):
 recovery.run();recovery.run(repair=False)
 assert int(r.get(counter))==44 and r.ttl(counter)==-1
 assert all(r.exists('pipeline:cache:alert:'+row.payload['alert_id']) for row in rows)
with factory() as db:
 assert all(db.get(Outbox,('alert',row.key,'redis')).done for row in rows)
# Cache writes serialize with rebuild; SQL ingestion continues during the pause.
entered=threading.Event();release=threading.Event();errors=[]
class Paused:
 def __getattr__(self,name):return getattr(r,name)
 def get(self,*args):
  if not entered.is_set():entered.set();assert release.wait(10)
  return r.get(*args)
def rebuild():
 try:CacheReconciler(factory,'alert',Paused()).run()
 except BaseException as exc:errors.append(exc)
t=threading.Thread(target=rebuild);t.start();assert entered.wait(10)
payload={'event_id':str(uuid.uuid4()),'event_type':'login','source_ip':'192.0.2.199',
 'user_id':'cache-recovery','timestamp':datetime.now(timezone.utc).isoformat(),
 'risk_score':91,'login_success':False}
consumer._handle_message('analytics.enriched.events',json.dumps(payload).encode())
# A second delivery must leave one durable receipt/business effect set.
consumer._handle_message('analytics.enriched.events',json.dumps(payload).encode())
consumer._outbox.next_reconciliation=time.monotonic()+60
consumer._outbox.cache_ready=True
w=threading.Thread(target=consumer._outbox.drain_once);w.start();time.sleep(.2)
assert w.is_alive(), 'Cache writer did not wait for repair lock'
release.set();t.join(15);w.join(15);assert not t.is_alive() and not w.is_alive() and not errors
recovery.run();recovery.run(repair=False)
with factory() as db:
 count=db.scalar(select(func.count()).select_from(Outbox).where(Outbox.service=='alert',Outbox.target=='redis',Outbox.payload['entity_id'].astext=='192.0.2.199'))
assert count>0 and int(r.get('counters:alerts:entity:192.0.2.199'))==count
# Expiry is durable, and repaired old payloads must not become permanent.
expired={**payload,'event_id':str(uuid.uuid4()),'source_ip':'192.0.2.198'}
consumer._handle_message('analytics.enriched.events',json.dumps(expired).encode())
with factory() as db,db.begin():
 expiring=db.scalars(select(Outbox).where(Outbox.service=='alert',Outbox.target=='redis',Outbox.payload['entity_id'].astext=='192.0.2.198')).all()
 assert expiring
 for row in expiring:row.payload={**row.payload,'_cache_expires_at':time.time()-10}
recovery.run()
assert r.get('latest:alert:192.0.2.198') is None
assert r.get('latest:alert:192.0.2.198:version') is not None
class SQLUnavailable:
 def __call__(self):raise ConnectionError('simulated SQL outage')
try:CacheReconciler(SQLUnavailable(),'alert',r).run()
except ConnectionError:pass
else:raise AssertionError('SQL outage reported cache correctness')
assert not r.exists('pipeline:cache:alert:verified')
class RedisUnavailable:
 def delete(self,*args):raise ConnectionError('simulated Redis outage')
try:CacheReconciler(factory,'alert',RedisUnavailable()).run()
except ConnectionError:pass
else:raise AssertionError('Redis outage reported cache correctness')
recovery.run()
print('44 counters/markers verified; completed effects, interruption, concurrency, expiry, dependency failures passed')
''')


def test_storage_projection_dimensions_and_bounded_repair():
    from pathlib import Path
    assert (Path(os.environ['HARDENED_SECRETS_DIR'])/'test-only').is_file()
    compose('exec','-T','storage-sink','python','-',timeout=180,input='''
from sqlalchemy import select,func
from pipeline_runtime.cache_recovery import CacheReconciler,require_complete_ledger
from pipeline_runtime.durable import Outbox,Receipt
from app.config.settings import get_settings
from app.db.session import create_session_factory
from app.redis_cache.cache import RedisCache
settings=get_settings();factory=create_session_factory(settings);r=RedisCache(settings)._client
repair=CacheReconciler(factory,'storage',r)
repair.run()
with factory() as db:
 row=db.execute(select(Outbox.key,Outbox.payload).where(Outbox.service=='storage',Outbox.target=='redis',Outbox.payload['event_type'].astext.isnot(None)).limit(1)).one()
 event_type=row.payload['event_type']
 expected=db.scalar(select(func.count()).select_from(Outbox).where(Outbox.service=='storage',Outbox.target=='redis',Outbox.payload['event_type'].astext==event_type))
with factory() as db:
 db.add(Receipt(service='storage',key=__import__('uuid').uuid4().hex,fingerprint='test-gap'));db.flush()
 try:require_complete_ledger(db,'storage')
 except ValueError:pass
 else:raise AssertionError('Incomplete storage ledger accepted')
 finally:db.rollback()
marker='pipeline:cache:storage:'+row.key;counter='counters:events:'+event_type
r.delete(marker);r.set(counter,'invalid')
try:repair.run(repair=False)
except ValueError:pass
else:raise AssertionError('Invalid storage counter accepted')
repair.run();repair.run(repair=False)
assert r.exists(marker) and int(r.get(counter))==expected
try:CacheReconciler(factory,'storage',r,max_records=0).run()
except ValueError:pass
else:raise AssertionError('Automatic replay record budget ignored')
assert int(r.get(counter))==expected
repair.run()
print('Storage counters, markers, latest/high-risk expiry and replay budget verified')
''')

"""Opt-in real Spark/Kafka restart and downstream SQL integrity test.

Uses new topics/checkpoints; never deletes historical state or resets groups.
"""
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
import pytest
from confluent_kafka import Consumer, TopicPartition, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import text

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT / 'tests/reliability'))
from support import engine, storage, alerts, module
from pipeline_runtime.kafka import publish_confirmed

pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.environ.get('SPARK_RECOVERY_INTEGRATION') != '1', reason='Enable isolated real Spark recovery integration explicitly')]


def read_all(topic):
    c = Consumer({'bootstrap.servers':'127.0.0.1:9092','group.id':'read-'+uuid.uuid4().hex,'enable.auto.commit':False})
    try:
        end = c.get_watermark_offsets(TopicPartition(topic,0),timeout=10)[1]
        c.assign([TopicPartition(topic,0,0)])
        result=[]; deadline=time.monotonic()+30
        while len(result)<end and time.monotonic()<deadline:
            m=c.poll(1)
            if m and not m.error(): result.append(json.loads(m.value()))
        assert len(result)==end
        return result
    finally:c.close()


def test_real_crash_replay_and_continuation(tmp_path):
    # A persistent ignored directory permits forensic review after an assertion fails.
    prefix='spark-recovery-'+uuid.uuid4().hex[:12]
    evidence=ROOT/'.codex-logs/phase2b'/prefix
    evidence.mkdir(parents=True)
    checkpoint=evidence/'checkpoints';checkpoint.mkdir()
    admin=AdminClient({'bootstrap.servers':'127.0.0.1:9092'})
    names=['login','api','network','anomaly','enriched','window','risk','features','predictions']
    for future in admin.create_topics([NewTopic(prefix+'.'+n,1,1) for n in names]).values():future.result(20)
    producer=Producer({'bootstrap.servers':'127.0.0.1:9092','delivery.timeout.ms':5000})
    start=datetime.now(timezone.utc).replace(second=0,microsecond=0)
    start=start.replace(minute=start.minute//5*5)
    expected=[]
    def seed(shift):
        ids=[]
        for kind,number in [('login',4),('api',4),('network',3),('anomaly',1)]:
            for i in range(number):
                event={'event_id':str(uuid.uuid4()),'event_type':kind,'timestamp':(start+timedelta(minutes=shift,seconds=10)).isoformat(),
                       'source_ip':'192.0.2.219','risk_score':80,'user_id':prefix,'country':'US','login_success':i%2==0,
                       'endpoint':'/recovery','status_code':[200,404,500,503][i%4],'response_time_ms':100,
                       'bytes_sent':100,'bytes_received':50,'destination_ip':'198.51.100.'+str(i%2+1),
                       'anomaly_type':'recovery','severity':'high'}
                ids.append(event['event_id']);expected.append(event['event_id'])
                publish_confirmed(producer,topic=prefix+'.'+kind,key=event['event_id'],value=json.dumps(event).encode())
            marker={**event,'event_id':str(uuid.uuid4()),'source_ip':'192.0.2.220',
                    'timestamp':(start+timedelta(minutes=shift+10)).isoformat()}
            publish_confirmed(producer,topic=prefix+'.'+kind,key=marker['event_id'],value=json.dumps(marker).encode())
        return ids
    first=seed(0)
    def run(label, crash=False):
        command=['docker','run','--rm','--user','root','--network','streaming-analytics-network',
                 '-v',str(ROOT/'spark-jobs/app')+':/opt/spark/jobs/app:ro',
                 '-v',str(ROOT/'tests/spark_recovery')+':/tests:ro',
                 '-v',str(checkpoint)+':/checkpoint',
                 '-e','RECOVERY_TEST_PREFIX='+prefix,'-e','RECOVERY_CRASH='+str(int(crash)),
                 'streaming_analytics_system-spark-streaming','/opt/spark/bin/spark-submit',
                 '--master','local[2]','--packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3','/tests/streaming_driver.py']
        with (evidence/(label+'.log')).open('w') as log:
            result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=240)
        assert result.returncode==(77 if crash else 0),str(evidence/(label+'.log'))
    run('crash',True)
    assert json.loads((checkpoint/'crash-evidence.json').read_text())['sink_written']
    assert (checkpoint/'features/offsets/1').exists() and not (checkpoint/'features/commits/1').exists()
    feature_identity=json.loads((checkpoint/'features/metadata').read_text())['id']
    run('replay')
    assert (checkpoint/'features/commits/1').exists()
    assert json.loads((checkpoint/'features/metadata').read_text())['id']==feature_identity
    second=seed(20)
    run('continue')
    data={name:read_all(prefix+'.'+name) for name in ['enriched','window','risk','features']}
    observed=[p['event_id'] for p in data['enriched'] if p['event_id'] in expected]
    assert sorted(observed)==sorted(expected)
    windows=[p for p in data['window'] if p['event_count']==4 or p['event_count'] in (3,1)]
    # Both five-minute test windows have exactly four event-type aggregates.
    starts={start.isoformat().replace('+00:00','Z'),(start+timedelta(minutes=20)).isoformat().replace('+00:00','Z')}
    selected=[p for p in data['window'] if datetime.fromisoformat(p['window_start'].replace('Z','+00:00')) in [start,start+timedelta(minutes=20)]]
    assert len(selected)==8 and sorted(p['event_count'] for p in selected)==[1,1,3,3,4,4,4,4]
    risks=[p for p in data['risk'] if p['entity_id']=='192.0.2.219']
    assert len(risks)==4 and all(p['rolling_event_count']==12 for p in risks)
    features=[p for p in data['features'] if p['entity_id']=='192.0.2.219']
    unique={(p['entity_id'],p['window_start'],p['window_end']):p for p in features}
    assert len(unique)==4 and len(features)==6  # two first-window messages replayed after the crash
    for p in features:
        assert sorted(p['source_event_ids']) in [sorted(first),sorted(second)]
        assert len(p['features'])==18 and p['features']['event_count']==12
        assert p['features']['failed_login_rate']==.5 and p['features']['api_5xx_rate']==.5
    from dotenv import dotenv_values
    env=dotenv_values(ROOT/'.env');m=module('ml-models','app.main')
    settings=importlib.import_module('app.config.settings').MLSettings(ML_ARTIFACT_DIR=Path(env['DEV_BIND_ROOT'])/'ml-models/artifacts/contract-v2',ML_PREDICTION_TOPIC=prefix+'.predictions')
    inference=m.MLInferenceConsumer(settings,m.AnomalyPredictor(settings,m.load_artifacts(settings.artifact_dir)))
    inference._producer=producer
    for p in features:inference._handle_message(prefix+'.features',json.dumps(p).encode())
    predictions=read_all(prefix+'.predictions')
    assert len(predictions)==6 and len({p['prediction_id'] for p in predictions})==4
    db=engine();sink=storage(db)
    for p in predictions:sink._handle_message('ml.anomaly.predictions',json.dumps(p).encode())
    alert_consumer=alerts(db)
    for p in predictions:alert_consumer._handle_message('ml.anomaly.predictions',json.dumps(p).encode())
    with db.connect() as c:
        for pid in {p['prediction_id'] for p in predictions}:
            assert c.scalar(text('SELECT count(*) FROM anomaly_predictions WHERE prediction_id=:id'),{'id':pid})==1
            assert c.scalar(text("SELECT count(*) FROM alerts WHERE raw_payload->>'prediction_id'=:id"),{'id':pid})==1
    summary={'prefix':prefix,'source_events_verified':len(expected),'window_metrics':len(selected),'risk_windows':len(risks),
             'feature_messages':len(features),'distinct_feature_windows':len(unique),'prediction_messages':len(predictions),
             'durable_predictions':4,'durable_alerts':4,'query_id_preserved':feature_identity}
    (evidence/'verified.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary))

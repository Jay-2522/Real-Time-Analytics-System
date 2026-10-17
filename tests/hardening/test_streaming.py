"""Explicit opt-in checks against only the new mTLS broker and Spark cluster."""
import os
import pytest
from test_stack import compose

pytestmark=[pytest.mark.integration,pytest.mark.skipif(os.environ.get('HARDENED_STREAMING_INTEGRATION')!='1',reason='Requires isolated secured messaging and compute services')]


def test_kafka_mtls_and_topic_acl():
    compose('run','--rm','-T','--entrypoint','python','telemetry-producer','-',input='''
import json,time,uuid
from confluent_kafka import Producer,Consumer,KafkaError
from infrastructure_runtime.security import kafka_options
options={'bootstrap.servers':'kafka:9092',**kafka_options()}
p=Producer({**options,'delivery.timeout.ms':5000})
errors=[]
p.produce('telemetry.login.events',key='security-'+uuid.uuid4().hex,value=json.dumps({'event_id':str(uuid.uuid4()),'event_type':'login','timestamp':'2026-10-17T00:00:00Z','source_ip':'192.0.2.2','user_id':'security-fixture','login_success':True}),on_delivery=lambda error,message:errors.append(error))
assert p.flush(10)==0 and errors==[None]
c=Consumer({**options,'group.id':'producer-forbidden','enable.auto.commit':False,'auto.offset.reset':'earliest'})
c.subscribe(['telemetry.login.events'])
try:
 deadline=time.monotonic()+12
 while time.monotonic()<deadline:
  m=c.poll(1)
  if m and m.error() and m.error().code() in (KafkaError.TOPIC_AUTHORIZATION_FAILED,KafkaError.GROUP_AUTHORIZATION_FAILED):break
 else:raise AssertionError('Producer identity was not denied read access')
finally:c.close()
# A trusted CA alone is insufficient: the broker requires a client certificate.
anonymous=Producer({'bootstrap.servers':'kafka:9092','security.protocol':'SSL','ssl.ca.location':'/run/security/ca.crt','socket.timeout.ms':3000})
try:anonymous.list_topics(timeout=4);raise AssertionError('Anonymous TLS client admitted')
except Exception as exc:
 assert not isinstance(exc,AssertionError)
print('Kafka verified mTLS delivery and denied anonymous/read access.')
''')


def test_spark_nonroot_authenticated_cluster_and_kafka_tls():
    compose('run','--rm','--use-aliases','-T','--entrypoint','/bin/bash','spark-driver','-ec',
            "cat > /tmp/security-check.py; exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-driver /tmp/security-check.py",input='''
import os,uuid
from pyspark.sql import SparkSession
from infrastructure_runtime.security import kafka_java_options
assert os.geteuid()==185
spark=SparkSession.builder.appName('IsolatedSecurityVerification').getOrCreate()
assert spark.sparkContext.getConf().get('spark.authenticate')=='true'
assert spark.sparkContext.getConf().get('spark.network.crypto.enabled')=='true'
assert spark.range(4).count()==4
frame=spark.read.format('kafka').option('kafka.bootstrap.servers','kafka:9092').options(**kafka_java_options()).option('subscribe','telemetry.login.events').option('startingOffsets','earliest').option('endingOffsets','latest').load()
assert frame.count()>0
path='/opt/spark/checkpoints/security-probe-'+uuid.uuid4().hex
os.mkdir(path)
with open(path+'/write-check','w') as file:file.write('non-root isolated checkpoint access')
spark.stop()
print('Non-root Spark RPC and Kafka TLS passed.')
''')

#!/usr/bin/env python3
"""Prepare isolated Kafka mTLS stores and Spark credentials from provisioned leaves."""
import argparse
import base64
import json
from pathlib import Path
import secrets
import subprocess
import uuid


def prepare(root):
    if (root/'kafka/cluster-id').exists():
        raise SystemExit('Messaging credentials already exist; use the documented rotation procedure')
    def write(path,value):
        path=root/path
        if path.exists():path.chmod(0o600)
        path.write_text(value);path.chmod(0o444)
    for name in ('kafka','kafka-admin','producer','spark','ml','storage','alerts','threat','consumer'):
        directory=root/name
        password=secrets.token_urlsafe(32)
        write(name+'/store-password',password+'\n')
        commands=[['openssl','pkcs12','-export','-in',str(directory/'server.crt'),'-inkey',str(directory/'server.key'),'-certfile',str(directory/'ca.crt'),'-out',str(directory/'server.p12')],
                  ['openssl','pkcs12','-export','-nokeys','-in',str(directory/'ca.crt'),'-jdktrust','anyExtendedKeyUsage','-out',str(directory/'truststore.p12')]]
        for command in commands:
            result=subprocess.run(command+['-passout','file:'+str(directory/'store-password')],capture_output=True)
            if result.returncode:raise SystemExit('PKCS12 generation failed; no credential output emitted')
        for filename in ('server.p12','truststore.p12'):(directory/filename).chmod(0o444)
        java={'security.protocol':'SSL','ssl.keystore.type':'PKCS12','ssl.keystore.location':'/run/security/server.p12',
              'ssl.keystore.password':password,'ssl.key.password':password,'ssl.truststore.type':'PKCS12',
              'ssl.truststore.location':'/run/security/truststore.p12','ssl.truststore.password':password,
              'ssl.endpoint.identification.algorithm':'https'}
        config=directory/'security.json'
        value=json.loads(config.read_text()) if config.exists() else {}
        value['kafka']={'security.protocol':'SSL','ssl.ca.location':'/run/security/ca.crt',
                        'ssl.certificate.location':'/run/security/server.crt','ssl.key.location':'/run/security/server.key'}
        value['kafka_java']={'kafka.'+k:v for k,v in java.items()}
        if name=='spark': value['kafka_java']['groupIdPrefix']='spark-hardened'
        write(name+'/security.json',json.dumps(value))
        write(name+'/client.properties','\n'.join(k+'='+v for k,v in java.items())+'\n')
    write('spark/metrics-token',(root/'backend/metrics-token').read_text())
    rpc=secrets.token_urlsafe(48)
    write('spark/spark-defaults.conf',f'spark.authenticate true\nspark.authenticate.secret {rpc}\nspark.network.crypto.enabled true\nspark.io.encryption.enabled true\nspark.ui.enabled false\nspark.master.rest.enabled false\nspark.local.dir /tmp/spark\nspark.driver.host spark-driver\nspark.executor.memory 512m\nspark.driver.memory 512m\n')
    write('kafka/cluster-id',base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')+'\n')
    print('Messaging identities prepared; existing broker data and consumer offsets were not accessed.')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--directory',type=Path,required=True)
    prepare(parser.parse_args().directory.resolve())

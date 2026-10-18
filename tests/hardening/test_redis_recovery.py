"""Real restricted-TLS Redis replay, including the original 44/22 defect.

Only disposable networks/volumes created here are mutated. No host ports or
retained deployment volumes are used. The selected fixed image must be built.
"""
import json
import os
import shutil
import subprocess
import time
import tempfile
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LIVE = os.environ.get('HARDENED_REDIS_RECOVERY') == '1'


def docker(*args, input=None, check=True):
    result = subprocess.run(['docker', *args], input=input, text=True,
                            capture_output=True, timeout=180)
    if check and result.returncode:
        directory = ROOT / '.codex-logs/phase4b'
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / ('redis-failure-' + uuid.uuid4().hex + '.log')
        path.write_text(result.stdout + result.stderr)
        raise AssertionError('Disposable Redis command failed; private diagnostics: ' + str(path))
    return result


@pytest.fixture
def redis_lab():
    if not LIVE:
        pytest.skip('Requires HARDENED_REDIS_RECOVERY=1 and isolated Docker endpoint')
    directory = ROOT / ".codex-logs/phase4b"
    directory.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(tempfile.mkdtemp(prefix="redis-lab-", dir=directory))
    source = Path(os.environ['HARDENED_SECRETS_DIR'])
    assert (source / 'test-only').is_file()
    name = 'soc-phase4b-aof-' + uuid.uuid4().hex[:12]
    shutil.copytree(source / 'redis', tmp_path / 'redis')
    config = tmp_path / 'redis/redis.conf'
    config.chmod(0o644)
    config.write_text(config.read_text() + '\nauto-aof-rewrite-percentage 0\nappendfsync everysec\naof-load-truncated no\n')
    # A separate test transaction identity has exactly the alert key scope.
    credentials = json.loads((source / 'alerts/security.json').read_text())
    password = credentials['redis']['password']
    acl = tmp_path / 'redis/users.acl'
    acl.chmod(0o644)
    acl.write_text(acl.read_text() + f'\nuser transaction on >{password} ~pipeline:cache:alert:* ~counters:alerts:* +multi +exec +set +incr +get\n')
    acl.chmod(0o444)
    docker('network', 'create', '--internal', name)
    docker('volume', 'create', name)
    docker('run', '--rm', '--network', 'none', '--cap-drop=ALL', '--cap-add=CHOWN',
           '-v', name + ':/data', 'alpine:3.23', 'chown', '999:999', '/data')

    def start(image):
        docker('run', '-d', '--name', name, '--network', name, '--network-alias', 'redis',
               '--user', '999:999', '--read-only', '--cap-drop=ALL', '--security-opt', 'no-new-privileges',
               '-v', name + ':/data', '-v', str(tmp_path / 'redis') + ':/run/tls:ro',
               image, 'redis-server', '/run/tls/redis.conf')
        for _ in range(30):
            if 'Ready to accept connections' in docker('logs', name).stdout:
                return
            time.sleep(.2)
        raise AssertionError('Redis did not finish AOF loading')

    def run(script):
        prefix = '''
import json,time,redis
from pathlib import Path
from pipeline_runtime.cache import cache_once
options=json.loads(Path('/run/security/security.json').read_text())['redis']
r=redis.Redis(host='redis',port=6379,ssl=True,decode_responses=True,**options)
admin_options={**options,'username':'redis-admin','password':json.loads(Path('/run/admin/bootstrap.json').read_text())['passwords']['redis-admin']}
a=redis.Redis(host='redis',port=6379,ssl=True,decode_responses=True,**admin_options)
'''
        return docker('run', '--rm', '-i', '--network', name, '--user', '10001:10001',
                      '--read-only', '--cap-drop=ALL', '--security-opt', 'no-new-privileges',
                      '-v', str(source / 'alerts') + ':/run/security:ro',
                      '-v', str(source / 'admin') + ':/run/admin:ro',
                      '--entrypoint', 'python', os.environ.get('HARDENED_REDIS_CLIENT_IMAGE', 'soc-phase4b-alert-engine'),
                      '-', input=prefix + script).stdout

    yield name, start, run
    docker('rm', '-f', name, check=False)
    docker('volume', 'rm', name, check=False)
    docker('network', 'rm', name, check=False)


def seed(first, last, expected=None):
    expected = last if expected is None else expected
    return f'''
for i in range({first},{last}):
 cache_once(r,'alert',str(i),'latest:alert:fixture','2026-10-18T00:00:00+00:00',
            {{'alert_id':str(i),'timestamp':'2026-10-18T00:00:00+00:00'}},['counters:alerts:entity:fixture'])
assert int(r.get('counters:alerts:entity:fixture'))=={expected}
'''


def check_count(count):
    return f'''
assert int(r.get('counters:alerts:entity:fixture'))=={count}
assert sum(r.exists('pipeline:cache:alert:'+str(i)) for i in range(44))=={count}
assert a.config_get('appendonly')['appendonly']=='yes'
assert a.config_get('appendfsync')['appendfsync']=='everysec'
try:r.set('unrelated:forbidden','x')
except redis.exceptions.NoPermissionError:pass
else:raise AssertionError('ACL key isolation lost')
try:redis.Redis(host='redis',port=6379,ssl=True,ssl_ca_certs=options['ssl_ca_certs']).ping()
except redis.exceptions.AuthenticationError:pass
else:raise AssertionError('Anonymous connection accepted')
print('counter={count} markers={count} restricted TLS verified')
'''


@pytest.mark.integration
@pytest.mark.parametrize('old', [True, False], ids=['affected-7.4.11', 'fixed-8.4.7'])
def test_restricted_aof_replay_and_copied_upgrade(redis_lab, old):
    name, start, run = redis_lab
    fixed = os.environ.get('HARDENED_REDIS_FIXED_IMAGE', 'soc-phase4b-redis')
    start(os.environ.get('HARDENED_REDIS_AFFECTED_IMAGE','redis:7.4.11-alpine') if old else fixed)
    run("assert a.info('server')['redis_version']==" + repr('7.4.11' if old else '8.4.7'))
    run(seed(0, 22) + '''
a.bgrewriteaof()
while a.info('persistence')['aof_rewrite_in_progress']:time.sleep(.1)
assert a.info('persistence')['aof_last_bgrewrite_status']=='ok'
''')
    run(seed(22, 44) + '''
tx=redis.Redis(host='redis',port=6379,ssl=True,decode_responses=True,**{**options,'username':'transaction'})
with tx.pipeline() as p:p.set('pipeline:cache:alert:transaction','1');p.execute_command('INCR','counters:alerts:transaction');p.execute()
r.set('pipeline:cache:alert:plain','durable')
time.sleep(2) # Bound the everysec policy before asserting expected durable writes.
''')
    for signal in ['TERM', 'KILL', 'KILL']:
        docker('kill', '--signal', signal, name)
        docker('wait', name)
        docker('start', name)
        time.sleep(.5)
        logs = docker('logs', name).stdout + docker('logs', name).stderr
        assert ('NOPERM' in logs) == old
        run(check_count(22 if old else 44) + '''
assert r.get('pipeline:cache:alert:plain')=='durable'
''')
    # The old incremental AOF still contains the skipped transactions. Reopen the
    # disposable lab volume on the fixed runtime, never the retained dev volume.
    if old:
        docker('stop', name)
        docker('rm', name)
        start(fixed)
        run(check_count(44))
    run('''
assert r.get('pipeline:cache:alert:transaction')=='1'
assert int(r.get('counters:alerts:transaction'))==1
''')


@pytest.mark.integration
def test_truncated_aof_refuses_loading_and_backup_restores(redis_lab):
    name, start, run = redis_lab
    start(os.environ.get('HARDENED_REDIS_FIXED_IMAGE', 'soc-phase4b-redis'))
    run(seed(0,44) + '\ntime.sleep(2)\n')
    docker('stop',name)
    image=os.environ.get('HARDENED_REDIS_CLIENT_IMAGE','soc-phase4b-alert-engine')
    def edit(script):
        docker('run','--rm','-i','--network','none','--user','999:999',
               '--read-only','--cap-drop=ALL','--security-opt','no-new-privileges',
               '-v',name+':/data','--entrypoint','python',image,'-',input=script)
    edit('''
from pathlib import Path
p=next(Path('/data/appendonlydir').glob('*.incr.aof'))
b=p.read_bytes();p.with_suffix('.test-backup').write_bytes(b)
p.write_bytes(b[:-3])
''')
    docker('start',name)
    for _ in range(30):
        if docker('inspect','--format','{{.State.Running}}',name).stdout.strip()=='false':break
        time.sleep(.2)
    else:raise AssertionError('Redis accepted a deliberately truncated AOF')
    logs=(docker('logs',name).stdout+docker('logs',name).stderr).lower()
    assert any(x in logs for x in ['unexpected end','short read','bad file format'])
    edit('''
from pathlib import Path
p=next(Path('/data/appendonlydir').glob('*.incr.aof'))
p.write_bytes(p.with_suffix('.test-backup').read_bytes())
''')
    docker('start',name);time.sleep(.5)
    run(check_count(44))


@pytest.mark.integration
@pytest.mark.parametrize('old', [True, False], ids=['phase4a-7.4.11', 'phase4b-8.4.7'])
def test_completed_before_restart_pending_after_restart(redis_lab, old):
    name, start, run = redis_lab
    start(os.environ.get('HARDENED_REDIS_AFFECTED_IMAGE','redis:7.4.11-alpine') if old else os.environ.get('HARDENED_REDIS_FIXED_IMAGE','soc-phase4b-redis'))
    run(seed(0,22)+'\ntime.sleep(2)\n')
    # First batch effects had completed in SQL before Redis went down. The second
    # batch's SQL outbox remains pending until Redis reconnects, as in Phase 4A.
    docker('stop',name);docker('start',name);time.sleep(.5)
    run("assert int(r.get('counters:alerts:entity:fixture') or 0)=="+str(0 if old else 22))
    run(seed(22,44,expected=22 if old else 44))
    run(check_count(22 if old else 44))
    run("assert all(bool(r.exists('pipeline:cache:alert:'+str(i)))=="+repr(not old)+" for i in range(22))")
    run("assert all(r.exists('pipeline:cache:alert:'+str(i)) for i in range(22,44))")


@pytest.mark.integration
def test_critical_list_uses_the_same_stable_order_as_reconstruction(redis_lab):
    _, start, run = redis_lab
    start(os.environ.get('HARDENED_REDIS_FIXED_IMAGE','soc-phase4b-redis'))
    run('''
from pipeline_runtime.cache_recovery import effects
for identity,stamp in [('utc','2026-10-18T00:00:00Z'),('offset','2026-10-18T00:00:00+05:30')]:
 body={'alert_id':identity,'entity_id':'fixture','severity':'critical','timestamp':stamp}
 effect=effects('alert',identity,body)[0]
 cache_once(r,*effect[:4],effect[4],effect[5],list_key=effect[6])
assert [json.loads(x)['alert_id'] for x in r.lrange('live:critical-alerts',0,50)]==['offset','utc']
''')

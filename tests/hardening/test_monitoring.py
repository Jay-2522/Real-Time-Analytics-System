"""Verify real exporters and authenticated monitoring APIs after upgrades."""
import os
import subprocess
from pathlib import Path

import pytest
from test_stack import compose

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.environ.get('HARDENED_MONITORING_INTEGRATION') != '1',
    reason='Requires isolated hardened monitoring and all worker exporters')]


def test_authorized_scrapes_and_grafana_datasource():
    directory = Path(os.environ['HARDENED_SECRETS_DIR']).resolve()
    assert (directory/'test-only').is_file()
    image=subprocess.check_output(['docker','inspect','--format','{{.Image}}',compose('ps','-q','backend').strip()],text=True).strip()
    result=subprocess.run(['docker','run','--rm','-i','--network','soc-hardened_monitoring',
            '--read-only','--cap-drop=ALL','--security-opt=no-new-privileges',
            '-e','ENVIRONMENT=production','-e','SERVICE_SECURITY_FILE=/run/security/security.json',
            '-v',str(directory/'backend')+':/run/security:ro',
            '-v',str(directory/'grafana')+':/run/grafana:ro',
            '--entrypoint','python',image,'-'],text=True,capture_output=True,timeout=150,input='''
import base64,json,time,urllib.request,urllib.error
from pathlib import Path
from infrastructure_runtime.security import section

def get(url,username=None,password=None):
 request=urllib.request.Request(url)
 if username:request.add_header('Authorization','Basic '+base64.b64encode((username+':'+password).encode()).decode())
 return json.load(urllib.request.urlopen(request,timeout=10))

credentials=section('prometheus')
expected={'backend','telemetry-producer','telemetry-consumer','ml-inference','storage-sink','alert-engine','threat-intelligence-engine'}
deadline=time.monotonic()+90
while time.monotonic()<deadline:
 rows=get('http://prometheus:9090/api/v1/query?query=up',**credentials)['data']['result']
 if {r['metric']['job'] for r in rows if r['value'][1]=='1'}==expected:break
 time.sleep(3)
else:raise AssertionError('Not all configured exporters scrape successfully')
assert all('instance' in r['metric'] for r in rows)
for host in ('http://prometheus:9090/api/v1/query?query=up','http://grafana:3000/api/datasources'):
 try:get(host);raise AssertionError('Anonymous monitoring access allowed')
 except urllib.error.HTTPError as e:assert e.code==401
password=Path('/run/grafana/password').read_text().strip()
sources=get('http://grafana:3000/api/datasources','admin',password)
source=next(s for s in sources if s['type']=='prometheus')
assert source['url']=='http://prometheus:9090' and source['basicAuth']
rows=get('http://grafana:3000/api/datasources/proxy/uid/'+source['uid']+'/api/v1/query?query=up','admin',password)['data']['result']
assert {r['metric']['job'] for r in rows if r['value'][1]=='1'}==expected
print('All seven configured targets scrape; authenticated Grafana queries the provisioned datasource; anonymous monitoring API access denied.')
''')
    assert result.returncode==0, 'Authenticated scrape/Grafana verification failed'

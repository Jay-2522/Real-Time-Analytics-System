"""Pure preflight tests never mutate real Spark state."""
import importlib.util
import json
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('checkpoint_guard', Path(__file__).parents[2] / 'spark-jobs/app/utils/checkpoints.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def write(root, kind, batch, content):
    target = root / kind / str(batch)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)


def offset(root, batch):
    write(root, 'offsets', batch, 'v1\n{}\n' + '\n'.join(['{"input":{"0":10}}'] * 4))


def commit(root, batch):
    write(root, 'commits', batch, 'v1\n{"nextBatchWatermarkMs":0}')


def test_complete_and_uncommitted_tail(tmp_path):
    offset(tmp_path, 0); commit(tmp_path, 0)
    guard.inspect_checkpoint(tmp_path)
    offset(tmp_path, 1)
    assert max(guard.inspect_checkpoint(tmp_path)['commits']) == 0


@pytest.mark.parametrize('defect', ['empty_offset', 'empty_commit', 'gap', 'commit_ahead', 'missing_state'])
def test_unsafe_checkpoint_rejected_without_rewrite(tmp_path, defect):
    offset(tmp_path, 0); commit(tmp_path, 0)
    if defect == 'empty_offset': write(tmp_path, 'offsets', 1, '')
    if defect == 'empty_commit': write(tmp_path, 'commits', 0, '')
    if defect == 'gap': offset(tmp_path, 2)
    if defect == 'commit_ahead': commit(tmp_path, 1)
    before = {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    with pytest.raises(guard.UnsafeCheckpoint):
        guard.inspect_checkpoint(tmp_path, stateful=defect == 'missing_state')
    assert before == {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}


def test_query_contract_and_namespace_isolation(tmp_path):
    for name in ['enriched', 'window', 'risk', 'features_v2']:
        guard.prepare_checkpoint(tmp_path / name, {'query': name}, False)
    with pytest.raises(guard.UnsafeCheckpoint):
        guard.prepare_checkpoint(tmp_path / 'risk', {'query': 'features_v2'}, False)
    guard.prepare_checkpoint(tmp_path / 'risk', {'query': 'risk'}, False)


def test_legacy_checkpoint_not_silently_adopted(tmp_path):
    offset(tmp_path, 0); commit(tmp_path, 0)
    with pytest.raises(guard.UnsafeCheckpoint): guard.prepare_checkpoint(tmp_path, {}, False)
    assert not (tmp_path / 'application-contract.json').exists()


def test_insufficient_retention_is_not_clamped():
    guard.validate_retention({'t': {'0': 4}}, {'t': {'0': [4, 10]}})
    for required in [3, 11]:
        with pytest.raises(guard.UnsafeCheckpoint):
            guard.validate_retention({'t': {'0': required}}, {'t': {'0': [4, 10]}})


def test_unsupported_uri_and_orphan_state_fail_closed(tmp_path):
    with pytest.raises(guard.UnsafeCheckpoint): guard.inspect_checkpoint('hdfs://cluster/checkpoints')
    (tmp_path / 'state').mkdir()
    (tmp_path / 'state' / 'orphan').write_bytes(b'state')
    with pytest.raises(guard.UnsafeCheckpoint): guard.inspect_checkpoint(tmp_path)

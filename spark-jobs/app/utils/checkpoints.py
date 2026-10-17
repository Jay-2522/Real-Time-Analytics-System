"""Conservative, read-only preflight for this application's Spark 3.5 checkpoints.

This detects unsafe metadata; it never repairs Spark's progress or state files.
Spark still performs the authoritative state-schema and delta validation on restart.
"""
import hashlib
import json
from pathlib import Path


class UnsafeCheckpoint(RuntimeError):
    pass


def local_checkpoint_path(path):
    root = Path(path)
    if not root.is_absolute() or "://" in str(path):
        raise UnsafeCheckpoint(f'Preflight supports absolute local/shared-volume paths only: {path}')
    return root


def inspect_checkpoint(path, sources=4, stateful=False, partitions=6):
    root = local_checkpoint_path(path)
    result = {"path": str(root), "offsets": {}, "commits": {}}
    for kind in ("offsets", "commits"):
        for file in sorted((root / kind).glob('*')):
            if not file.name.isdigit():
                continue
            try:
                lines = file.read_text().splitlines()
                if not lines or lines[0] != 'v1':
                    raise ValueError('missing v1 header')
                metadata = json.loads(lines[1])
                if kind == 'offsets':
                    if len(lines) != sources + 2:
                        raise ValueError('source count differs')
                    offsets = [json.loads(line) if line != '-' else None for line in lines[2:]]
                    if not all(isinstance(v, dict) or v is None for v in offsets):
                        raise ValueError('invalid source offsets')
                    metadata = {**metadata, 'sources': offsets}
                elif len(lines) != 2 or 'nextBatchWatermarkMs' not in metadata:
                    raise ValueError('invalid commit metadata')
                result[kind][int(file.name)] = metadata
            except (ValueError, IndexError, UnicodeError) as exc:
                raise UnsafeCheckpoint(f'{file}: incomplete/invalid metadata: {exc}') from exc
        ids = sorted(result[kind])
        if ids and ids != list(range(ids[0], ids[-1] + 1)):
            raise UnsafeCheckpoint(f'{root}/{kind}: gap in retained history')
    offsets, commits = result['offsets'], result['commits']
    if not offsets and any((root / 'state').rglob('*')):
        raise UnsafeCheckpoint(f'{root}: state exists without offset history')
    if commits and not offsets:
        raise UnsafeCheckpoint(f'{root}: commits without offsets')
    if offsets:
        latest = max(offsets)
        committed = max(commits, default=-1)
        if latest - committed not in (0, 1):
            raise UnsafeCheckpoint(f'{root}: inconsistent offset/commit tail {latest}/{committed}')
        if stateful and committed >= 0:
            # This application's three stateful queries each have one aggregation operator.
            # Version committed+1 is needed to continue; uncommitted later deltas may exist.
            version = committed + 1
            for partition in range(partitions):
                directory = root / 'state' / '0' / str(partition)
                candidates = [directory / f'{version}.delta', directory / f'{version}.snapshot']
                if not any(f.is_file() and f.stat().st_size > 0 for f in candidates):
                    raise UnsafeCheckpoint(f'{directory}: missing/empty state version {version}')
            if not (root / 'state/0/0/_metadata/schema').is_file():
                raise UnsafeCheckpoint(f'{root}: missing state schema')
    return result


def validate_retention(required, available):
    """Compare exact next offsets with broker [earliest, latest] bounds; never clamp."""
    for topic, partitions in required.items():
        for partition, offset in partitions.items():
            bounds = available.get(topic, {}).get(str(partition))
            if bounds is None or not bounds[0] <= offset <= bounds[1]:
                raise UnsafeCheckpoint(f'Unavailable source offset {topic}/{partition}/{offset}: {bounds}')


def contract_signature(settings, topic, query_name, schema, source_topics=None):
    # Explicit semantic version plus source fingerprints catches same-schema logic changes.
    base = Path(__file__).parents[1]
    modules = ['aggregations/window_metrics.py', 'feature_engineering/features.py',
               'transformations/normalization.py', 'schemas/telemetry.py']
    return {'version': 1, 'spark': '3.5.3', 'query': query_name, 'output': topic,
            'watermark': settings.watermark_delay, 'shuffle_partitions': 6,
            'sources': source_topics or ['telemetry.login.events', 'telemetry.api.events', 'telemetry.network.events', 'telemetry.anomaly.events'],
            'broker': settings.kafka_bootstrap_servers, 'schema': schema, 'logic': {p: hashlib.sha256((base / p).read_bytes()).hexdigest() for p in modules}}


def prepare_checkpoint(path, signature, stateful):
    root = local_checkpoint_path(path)
    inspect_checkpoint(root, stateful=stateful)
    manifest = root / 'application-contract.json'
    if manifest.exists():
        if json.loads(manifest.read_text()) != signature:
            raise UnsafeCheckpoint(f'{root}: incompatible application/query contract')
    elif any(root.glob('offsets/[0-9]*')):
        raise UnsafeCheckpoint(f'{root}: legacy checkpoint requires reviewed compatibility adoption; no automatic migration')
    else:
        root.mkdir(parents=True, exist_ok=True)
        # Exclusive creation prevents concurrent writers establishing differing contracts.
        with manifest.open('x') as stream:
            json.dump(signature, stream, sort_keys=True)

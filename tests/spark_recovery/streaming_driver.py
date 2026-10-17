"""Real Spark/Kafka regression driver. Only test namespaces; exits between sink and commit."""
import json
import os
from pathlib import Path
from pyspark.sql import functions as F
from app.config.settings import SparkStreamingSettings
from app.utils.spark import create_spark_session
from app.main import build_source_stream
from app.schemas.telemetry import login_event_schema, api_event_schema, network_event_schema, anomaly_event_schema
from app.transformations.normalization import select_enriched_columns
from app.aggregations.window_metrics import build_window_metrics, build_risk_metrics
from app.feature_engineering.features import build_feature_pipeline
from app.utils.checkpoints import prepare_checkpoint, contract_signature

prefix = os.environ['RECOVERY_TEST_PREFIX']
settings = SparkStreamingSettings(SPARK_CHECKPOINT_ROOT='/checkpoint', SPARK_STARTING_OFFSETS='earliest')
spark = create_spark_session(settings)
frames = [build_source_stream(spark, settings, prefix + '.' + name, schema) for name, schema in
          zip(['login', 'api', 'network', 'anomaly'], [login_event_schema, api_event_schema, network_event_schema, anomaly_event_schema])]
enriched = select_enriched_columns(frames[0])
for frame in frames[1:]: enriched = enriched.unionByName(select_enriched_columns(frame))
outputs = [('enriched', enriched), ('window', build_window_metrics(enriched, settings)),
           ('risk', build_risk_metrics(enriched, settings)), ('features', build_feature_pipeline(*frames, settings))]
try:
    for name, frame in outputs:
        path = '/checkpoint/' + name
        prepare_checkpoint(path, contract_signature(settings, prefix + '.' + name, name, frame.schema.json(), [prefix+'.'+n for n in ['login','api','network','anomaly']]), name != 'enriched')
        def sink(batch, batch_id, name=name):
            wire = batch.select(F.to_json(F.struct(*[F.col(c) for c in batch.columns]),
                               {'ignoreNullFields':'false','timeZone':'UTC'}).alias('value'))
            wire.write.format('kafka').option('kafka.bootstrap.servers', settings.kafka_bootstrap_servers).option('topic',prefix+'.'+name).save()
            if name == 'features' and os.environ.get('RECOVERY_CRASH') == '1' and batch_id == 1:
                Path('/checkpoint/crash-evidence.json').write_text(json.dumps({'query':name,'batch':batch_id,'sink_written':True}))
                os._exit(77)
        query = frame.writeStream.foreachBatch(sink).outputMode('append').option('checkpointLocation',path).trigger(availableNow=True).start()
        query.awaitTermination()
finally:
    spark.stop()

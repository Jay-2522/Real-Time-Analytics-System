from prometheus_client import Counter, Gauge, Histogram, start_http_server

IOC_ENRICHMENTS_TOTAL = Counter("ioc_enrichments_total", "IOC enrichments completed.", ["ioc_type", "severity"])
DETECTION_RULE_EXECUTIONS_TOTAL = Counter("detection_rule_executions_total", "Detection rule executions.", ["rule_id", "matched"])
DETECTION_RULE_HITS_TOTAL = Counter("detection_rule_hits_total", "Detection rule hits.", ["rule_id", "severity"])
DETECTION_RULE_LOADED = Gauge("detection_rule_loaded", "Detection rule loaded by the running threat engine.", ["rule_id", "rule_name", "severity"])
THREAT_EVENTS_PROCESSED_TOTAL = Counter("threat_events_processed_total", "Threat intelligence events processed.", ["source_topic"])
THREAT_ENGINE_ERRORS_TOTAL = Counter("threat_engine_errors_total", "Threat intelligence engine errors.", ["stage"])
THREAT_ENRICHMENT_LATENCY_SECONDS = Histogram("threat_enrichment_latency_seconds", "Threat enrichment latency.")

ANALYTICS_ENRICHED_EVENTS = "analytics.enriched.events"
ANALYTICS_WINDOW_METRICS = "analytics.window.metrics"
ANALYTICS_RISK_METRICS = "analytics.risk.metrics"
ANALYTICS_FEATURE_ENGINEERING = "analytics.feature.engineering"
ML_ANOMALY_PREDICTIONS = "ml.anomaly.predictions"
TELEMETRY_DEADLETTER_EVENTS = "telemetry.deadletter.events"

# Only atomic feature windows are valid model inputs.
ANALYTICS_INPUT_TOPICS = (ANALYTICS_FEATURE_ENGINEERING,)

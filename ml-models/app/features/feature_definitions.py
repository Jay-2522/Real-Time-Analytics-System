"""Versioned source-IP/60-second contract; order is also the scaler input order."""
FEATURE_CONTRACT_VERSION = "2.0"
FEATURE_COLUMNS = [
    "event_count", "login_event_count", "api_event_count", "network_event_count", "anomaly_event_count",
    "failed_login_rate", "failed_auth_count", "requests_per_minute", "avg_api_response_time",
    "endpoint_error_rate", "api_5xx_rate", "avg_network_bytes", "high_risk_network_event_count",
    "anomaly_rate", "risk_score_moving_average", "high_risk_event_count",
    "distinct_login_country_count", "unique_destination_ip_count",
]
NULLABLE_BY_COUNT = {
    "failed_login_rate": "login_event_count",
    "avg_api_response_time": "api_event_count",
    "endpoint_error_rate": "api_event_count",
    "api_5xx_rate": "api_event_count",
    "avg_network_bytes": "network_event_count",
}
PREPROCESSING = "conditional-null-to-zero-with-event-counts-v1;StandardScaler"
COUNT_COLUMNS = {name for name in FEATURE_COLUMNS if name.endswith("count")}
RATE_COLUMNS = {"failed_login_rate", "endpoint_error_rate", "api_5xx_rate", "anomaly_rate"}

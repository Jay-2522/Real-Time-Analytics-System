def explain_alert(rule_name: str, event_type: str, severity: str) -> tuple[str, str]:
    if rule_name == "api_server_error":
        return (
            "The API event reports a server-side HTTP error response.",
            "Review the endpoint logs and trace the failed request before deciding whether it is malicious.",
        )
    if rule_name == "large_network_transfer":
        return (
            "This single network event transferred at least eight million bytes.",
            "Validate the destination and expected transfer size before escalating.",
        )
    if rule_name == "high_anomaly":
        return (
            f"The ML detector classified this {event_type} signal as {severity} based on abnormal feature patterns.",
            "Review the related entity timeline and compare recent behavior against normal operating baselines.",
        )
    if rule_name == "model_positive":
        return (
            "The model returned an explicit positive anomaly decision even though its numeric scores were below the alert thresholds.",
            "Review the prediction features and related entity timeline to validate the model decision.",
        )
    return (
        "Correlated telemetry and risk signals indicate activity outside the expected baseline.",
        "Review the incident timeline, validate business context, and escalate if the activity is unauthorized.",
    )


"""Deterministic coherent window statistics, not an accuracy evaluation dataset."""
from __future__ import annotations
import numpy as np
import pandas as pd
from app.features.feature_definitions import FEATURE_COLUMNS
from app.features.feature_mapper import validate_features


def generate_synthetic_training_data(sample_size: int, anomaly_rate: float, random_state: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(random_state)
    rows = []
    for index in range(sample_size):
        suspicious = index < max(1, int(sample_size * anomaly_rate))
        # Event family absence is explicit; rates/averages are undefined without observations.
        counts = rng.poisson([12, 30, 15, 4] if suspicious else [3, 8, 5, .2])
        counts[rng.random(4) < .4] = 0
        if counts.sum() == 0:
            counts[rng.integers(4)] = 1
        login, api, network, anomaly = map(int, counts)
        n = int(counts.sum())
        failures = int(rng.binomial(login, .8 if suspicious else .06))
        errors = int(rng.binomial(api, .7 if suspicious else .05))
        server_errors = int(rng.binomial(errors, .7 if suspicious else .2))
        risks = np.clip(rng.normal(80 if suspicious else 25, 12, n), 0, 100)
        row = {
            "event_count": n, "login_event_count": login, "api_event_count": api,
            "network_event_count": network, "anomaly_event_count": anomaly,
            "failed_login_rate": failures / login if login else None,
            "failed_auth_count": failures, "requests_per_minute": api,
            "avg_api_response_time": float(rng.gamma(5, 220 if suspicious else 40)) if api else None,
            "endpoint_error_rate": errors / api if api else None,
            "api_5xx_rate": server_errors / api if api else None,
            "avg_network_bytes": float(rng.lognormal(11 if suspicious else 9, .5)) if network else None,
            "high_risk_network_event_count": int((risks[login+api:login+api+network] >= 70).sum()),
            "anomaly_rate": anomaly / n, "risk_score_moving_average": float(risks.mean()),
            "high_risk_event_count": int((risks >= 70).sum()),
            "distinct_login_country_count": int(len(set(rng.integers(0, 8 if suspicious else 2, login)))),
            "unique_destination_ip_count": int(len(set(rng.integers(0, 30 if suspicious else 3, network)))),
        }
        rows.append({**validate_features(row), "label": int(suspicious)})
    return pd.DataFrame(rows, columns=FEATURE_COLUMNS + ["label"]).sample(frac=1, random_state=random_state).reset_index(drop=True)

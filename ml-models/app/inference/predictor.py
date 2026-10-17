from __future__ import annotations

import numpy as np
import pandas as pd

from app.config.settings import MLSettings
from app.features.feature_definitions import FEATURE_COLUMNS
from app.models.artifact_store import ModelArtifacts, validate_artifacts
from app.schemas.prediction import PredictionEvent, Severity


class AnomalyPredictor:
    def __init__(self, settings: MLSettings, artifacts: ModelArtifacts) -> None:
        validate_artifacts(artifacts)
        self._settings = settings
        self._artifacts = artifacts

    def predict(self, features: dict[str, float], source_event_id: str | None, event_type: str, entity_id: str, window_metadata: dict | None = None) -> PredictionEvent:
        if list(features) != FEATURE_COLUMNS or not all(np.isfinite(v) for v in features.values()):
            raise ValueError("Invalid ordered model input")
        vector = pd.DataFrame([[features[name] for name in FEATURE_COLUMNS]], columns=FEATURE_COLUMNS, dtype=float)
        scaled = self._artifacts.scaler.transform(vector)

        decision_score = float(self._artifacts.model.decision_function(scaled)[0])
        prediction = int(self._artifacts.model.predict(scaled)[0])
        anomaly_score = self._to_anomaly_score(decision_score)
        ml_risk_score = round(min(100.0, max(0.0, anomaly_score * 100.0)), 4)
        severity = self._severity(ml_risk_score)
        is_anomaly = prediction == -1 or ml_risk_score >= 70.0

        return PredictionEvent(
            **(window_metadata or {}),
            source_event_id=source_event_id,
            model_name=self._artifacts.metadata["model_name"],
            model_version=self._artifacts.metadata["model_version"],
            model_artifact_id=self._artifacts.metadata["artifact_id"],
            event_type=event_type,
            entity_id=entity_id,
            anomaly_score=round(anomaly_score, 6),
            is_anomaly=is_anomaly,
            ml_risk_score=ml_risk_score,
            confidence_score=round(abs(anomaly_score - 0.5) * 2.0, 6),
            severity=severity,
            explanation=self._explanation(severity, features),
            features_used={name: round(float(features[name]), 6) for name in FEATURE_COLUMNS},
        )

    @staticmethod
    def _to_anomaly_score(decision_score: float) -> float:
        return float(1.0 / (1.0 + np.exp(8.0 * decision_score)))

    @staticmethod
    def _severity(risk_score: float) -> Severity:
        if risk_score >= 90:
            return "critical"
        if risk_score >= 75:
            return "high"
        if risk_score >= 45:
            return "medium"
        return "low"

    @staticmethod
    def _explanation(severity: Severity, features: dict[str, float]) -> str:
        ranked = sorted(features.items(), key=lambda item: item[1], reverse=True)[:3]
        drivers = ", ".join(name for name, _value in ranked)
        return f"Isolation Forest assigned {severity} severity using strongest feature signals: {drivers}"

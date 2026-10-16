from __future__ import annotations

import logging
from datetime import UTC, datetime

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

from app.config.settings import MLSettings, get_settings
from app.data.synthetic_training_data import generate_synthetic_training_data
from app.features.feature_definitions import FEATURE_COLUMNS, FEATURE_CONTRACT_VERSION, PREPROCESSING
from app.models.artifact_store import save_artifacts
from app.utils.logging import configure_logging

logger = logging.getLogger(__name__)


def train_and_save(settings: MLSettings) -> dict:
    dataset = generate_synthetic_training_data(
        sample_size=settings.training_sample_size,
        anomaly_rate=settings.contamination,
    )
    x = dataset[FEATURE_COLUMNS]
    y = dataset["label"].to_numpy()

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    model = IsolationForest(
        n_estimators=250,
        contamination=settings.contamination,
        random_state=42,
        max_samples="auto",
        n_jobs=-1,
    )
    model.fit(x_scaled)

    raw_predictions = model.predict(x_scaled)
    predicted_labels = np.where(raw_predictions == -1, 1, 0)
    report = classification_report(y, predicted_labels, output_dict=True, zero_division=0)
    matrix = confusion_matrix(y, predicted_labels).tolist()

    metadata = {
        "model_name": settings.model_name,
        "model_version": settings.model_version,
        "algorithm": "IsolationForest",
        "feature_columns": FEATURE_COLUMNS,
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "preprocessing": PREPROCESSING,
        "entity_type": "source_ip",
        "window_seconds": 60,
        "random_state": 42,
        "training_data": "synthetic coherent source-IP windows; no independent accuracy evaluation",
        "created_at": datetime.now(UTC).isoformat(),
        "training_sample_size": int(settings.training_sample_size),
        "contamination": float(settings.contamination),
    }
    metrics = {
        "model_name": settings.model_name,
        "model_version": settings.model_version,
        "classification_report": report,
        "confusion_matrix": matrix,
        "anomaly_rate_observed": float(predicted_labels.mean()),
        "feature_means": {column: float(x[column].mean()) for column in FEATURE_COLUMNS},
    }

    save_artifacts(settings.artifact_dir, model, scaler, metadata, metrics)
    logger.info(
        "saved_model_artifacts artifact_dir=%s sample_size=%s anomaly_rate_observed=%.4f",
        settings.artifact_dir,
        settings.training_sample_size,
        metrics["anomaly_rate_observed"],
    )
    return metrics


def main() -> None:
    settings = get_settings()
    configure_logging(settings.inference_log_level)
    train_and_save(settings)


if __name__ == "__main__":
    main()


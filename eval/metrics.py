"""Binary window metrics, benign alert burden, and first-detection latency."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

from eval.detectors import Detector


def metric_row(detector: str, scope: str, y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, object]:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[False, True]).ravel()
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0
    return {
        "detector": detector,
        "scope": scope,
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": fpr,
    }


def benign_alerts_per_hour(windows: pd.DataFrame, predictions: np.ndarray) -> tuple[float, int, int]:
    """Count alerts in wall-clock minutes containing no attack-labeled window."""
    minute_has_attack = windows.groupby("window_start", observed=True)["is_attack"].any()
    benign_minutes = set(minute_has_attack.index[~minute_has_attack])
    benign_only_rows = windows["window_start"].isin(benign_minutes).to_numpy()
    false_alerts = int((predictions & benign_only_rows).sum())
    hours = len(benign_minutes) / 60
    return (false_alerts / hours if hours else 0.0), false_alerts, len(benign_minutes)


def evaluate_days(
    detectors: Iterable[Detector],
    test_days: dict[str, pd.DataFrame],
) -> tuple[list[dict[str, object]], dict[str, np.ndarray], list[dict[str, object]]]:
    metrics: list[dict[str, object]] = []
    predictions: dict[str, np.ndarray] = {}
    accuracy_context: list[dict[str, object]] = []
    for day, windows in test_days.items():
        y_true = windows["is_attack"].to_numpy(dtype=bool)
        for detector in detectors:
            key = f"{detector.name}|{day}"
            y_pred = detector.predict(windows).astype(bool)
            predictions[key] = y_pred
            row = metric_row(detector.name, day, y_true, y_pred)
            alerts_per_hour, benign_alerts, benign_minutes = benign_alerts_per_hour(windows, y_pred)
            row.update(
                alerts_per_benign_hour=alerts_per_hour,
                benign_only_alerts=benign_alerts,
                benign_only_minutes=benign_minutes,
            )
            metrics.append(row)
            if detector.name == "All benign baseline":
                accuracy_context.append(
                    {
                        "day": day,
                        "all_benign_accuracy_context": float((~y_true).mean()),
                        "missed_attack_windows": int(y_true.sum()),
                    }
                )
    return metrics, predictions, accuracy_context


def evaluate_attack_types(
    detectors: Iterable[Detector],
    test_days: dict[str, pd.DataFrame],
    predictions: dict[str, np.ndarray],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    type_metrics: list[dict[str, object]] = []
    latencies: list[dict[str, object]] = []
    for day, windows in test_days.items():
        attack_types = sorted({attack for labels in windows["attack_types"] for attack in labels})
        benign = ~windows["is_attack"].to_numpy(dtype=bool)
        for attack_type in attack_types:
            target = windows["attack_types"].map(lambda labels: attack_type in labels).to_numpy(dtype=bool)
            evaluation_mask = benign | target
            for detector in detectors:
                y_pred = predictions[f"{detector.name}|{day}"]
                row = metric_row(detector.name, f"{day}: {attack_type}", target[evaluation_mask], y_pred[evaluation_mask])
                type_metrics.append(row)
                # Latency is the number of this type's attack windows missed
                # before its first positive; zero means the first was detected.
                attack_predictions = y_pred[target]
                detected = np.flatnonzero(attack_predictions)
                latencies.append(
                    {
                        "detector": detector.name,
                        "day": day,
                        "attack_type": attack_type,
                        "attack_windows": int(target.sum()),
                        "latency_windows": int(detected[0]) if len(detected) else "missed",
                    }
                )
    return type_metrics, latencies

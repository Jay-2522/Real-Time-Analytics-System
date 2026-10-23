from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from app.schemas.threat import RuleHit
from app.utils.metrics import DETECTION_RULE_EXECUTIONS_TOTAL, DETECTION_RULE_HITS_TOTAL, DETECTION_RULE_LOADED

logger = logging.getLogger(__name__)


class DetectionRuleEngine:
    def __init__(self, rules_path: str) -> None:
        self._rules_path = Path(rules_path)
        self._rules: list[dict[str, Any]] = []
        self.reload()

    def reload(self) -> None:
        rules: list[dict[str, Any]] = []
        for path in sorted(self._rules_path.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as handle:
                rule = yaml.safe_load(handle) or {}
            if self._valid(rule):
                rules.append(rule)
            else:
                logger.warning("invalid_detection_rule path=%s", path)
        self._rules = rules
        DETECTION_RULE_LOADED.clear()
        for rule in rules:
            DETECTION_RULE_LOADED.labels(str(rule["id"]), str(rule["name"]), str(rule["severity"])).set(1)
        logger.info("detection_rules_loaded count=%s path=%s", len(rules), self._rules_path)

    def evaluate(self, payload: dict[str, Any], risk_score: float) -> list[RuleHit]:
        hits: list[RuleHit] = []
        for rule in self._rules:
            rule_id = str(rule["id"])
            conditions = rule.get("conditions", {})
            min_risk = float(conditions.get("min_risk_score", 0))
            matched = risk_score >= min_risk and self._matches_fields(payload, conditions)
            DETECTION_RULE_EXECUTIONS_TOTAL.labels(rule_id, str(matched).lower()).inc()
            if matched:
                hit = RuleHit(rule_id=rule_id, rule_name=rule["name"], severity=rule["severity"], tactic=rule["tactic"], technique=rule["technique"], mitre_id=rule["mitre_id"], score=float(rule.get("score", min_risk)))
                hits.append(hit)
                DETECTION_RULE_HITS_TOTAL.labels(rule_id, hit.severity).inc()
        return hits

    @classmethod
    def _matches_fields(cls, payload: dict[str, Any], conditions: dict[str, Any]) -> bool:
        for field, expected in conditions.get("field_equals", {}).items():
            values = expected if isinstance(expected, list) else [expected]
            if not any(cls._equal(payload.get(field), value) for value in values):
                return False
        for field, expected in conditions.get("field_contains", {}).items():
            values = expected if isinstance(expected, list) else [expected]
            actual = payload.get(field)
            if actual is None or not any(str(value).lower() in str(actual).lower() for value in values):
                return False
        return True

    @staticmethod
    def _equal(actual: Any, expected: Any) -> bool:
        if isinstance(actual, str) and isinstance(expected, str):
            return actual.casefold() == expected.casefold()
        return actual == expected

    @staticmethod
    def _valid(rule: dict[str, Any]) -> bool:
        required = {"id", "name", "enabled", "severity", "tactic", "technique", "mitre_id", "conditions"}
        return required.issubset(rule.keys()) and bool(rule.get("enabled"))

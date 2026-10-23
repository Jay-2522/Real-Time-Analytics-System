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
        text = str(payload).lower()
        country = str(payload.get("country") or payload.get("geo_country") or "")
        hits: list[RuleHit] = []
        for rule in self._rules:
            rule_id = str(rule["id"])
            conditions = rule.get("conditions", {})
            keywords = [str(keyword).lower() for keyword in conditions.get("keywords", [])]
            min_risk = float(conditions.get("min_risk_score", 0))
            countries = set(conditions.get("countries", []))
            matched = risk_score >= min_risk and (not keywords or any(keyword in text for keyword in keywords))
            if countries and country not in countries:
                matched = False
            DETECTION_RULE_EXECUTIONS_TOTAL.labels(rule_id, str(matched).lower()).inc()
            if matched:
                hit = RuleHit(rule_id=rule_id, rule_name=rule["name"], severity=rule["severity"], tactic=rule["tactic"], technique=rule["technique"], mitre_id=rule["mitre_id"], score=float(rule.get("score", min_risk)))
                hits.append(hit)
                DETECTION_RULE_HITS_TOTAL.labels(rule_id, hit.severity).inc()
        return hits

    @staticmethod
    def _valid(rule: dict[str, Any]) -> bool:
        required = {"id", "name", "enabled", "severity", "tactic", "technique", "mitre_id", "conditions"}
        return required.issubset(rule.keys()) and bool(rule.get("enabled"))

import sys
from pathlib import Path


def load_threat_module(module: str):
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    return __import__(module, fromlist=["*"])


def test_ioc_enrichment_detects_tor_ip_and_geo():
    module = load_threat_module("app.enrichment.ioc_enricher")
    findings, country, asn = module.IOCEnricher().enrich({"source_ip": "185.220.101.1", "country": "Russia"})
    assert country == "Russia"
    assert asn == "AS-TOR Local Intelligence"
    assert any(finding.feed_name == "tor-exit-nodes" for finding in findings)


def test_detection_rule_parser_and_execution(tmp_path):
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    (rules_dir / "test.yaml").write_text(
        """
id: test_rule
name: Test Rule
description: test
enabled: true
severity: high
tactic: Credential Access
technique: Brute Force
mitre_id: T1110
conditions:
  field_equals:
    event_type: [login]
    login_success: [false]
  min_risk_score: 50
score: 80
""",
        encoding="utf-8",
    )
    module = load_threat_module("app.detections.rule_engine")
    hits = module.DetectionRuleEngine(str(rules_dir)).evaluate({"event_type": "login", "login_success": False}, 72)
    assert len(hits) == 1
    assert hits[0].mitre_id == "T1110"


def test_unrelated_payload_text_does_not_trigger_structured_rules(tmp_path):
    rules_dir = tmp_path / "rules"
    rules_dir.mkdir()
    (rules_dir / "test.yaml").write_text(
        """
id: structured_rule
name: Structured Rule
description: test
enabled: true
severity: high
tactic: Execution
technique: Command and Scripting Interpreter
mitre_id: T1059
conditions:
  field_contains:
    process_name: [powershell]
  min_risk_score: 1
score: 80
""",
        encoding="utf-8",
    )
    module = load_threat_module("app.detections.rule_engine")
    engine = module.DetectionRuleEngine(str(rules_dir))

    assert engine.evaluate({"processed_at": "now", "feature_name": "powershell_login_rate"}, 99) == []
    assert len(engine.evaluate({"process_name": "PowerShell.exe"}, 99)) == 1


def test_threat_scoring_escalates_with_iocs_and_rules():
    schemas = load_threat_module("app.schemas.threat")
    scorer_module = load_threat_module("app.scoring.threat_scorer")
    score, confidence, severity = scorer_module.ThreatScorer().score(
        70,
        [schemas.IOCFinding(value="1.1.1.1", ioc_type="ip", feed_name="test", confidence=0.9, severity="high", description="test")],
        [schemas.RuleHit(rule_id="r1", rule_name="R1", severity="high", tactic="Credential Access", technique="Brute Force", mitre_id="T1110", score=84)],
        82,
    )
    assert score >= 70
    assert confidence > 0.5
    assert severity in {"medium", "high", "critical"}

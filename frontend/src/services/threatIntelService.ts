import { httpClient } from "../api/httpClient";
import type { AttackTimeline, DetectionRuleSummary, EntityProfile, IOCMatch, LoadedDetectionRule, MitreTechnique, ThreatGraph, ThreatOverview } from "../types/threatIntel";

export const threatIntelService = {
  async overview() {
    return get<ThreatOverview>("/threat-intel/overview");
  },
  async iocs(limit = 100) {
    return get<IOCMatch[]>(`/threat-intel/iocs?limit=${limit}`);
  },
  async entities(limit = 100) {
    return get<EntityProfile[]>(`/threat-intel/entities?limit=${limit}`);
  },
  async highRiskEntities(limit = 50) {
    return get<EntityProfile[]>(`/entities/high-risk?limit=${limit}`);
  },
  async entity(entityId: string) {
    return get<EntityProfile | null>(`/entities/${encodeURIComponent(entityId)}`);
  },
  async mitre() {
    return get<MitreTechnique[]>("/threat-intel/mitre");
  },
  async tactics() {
    return get<string[]>("/mitre/tactics");
  },
  async techniques() {
    return get<MitreTechnique[]>("/mitre/techniques");
  },
  async attackTimeline(entityId?: string) {
    const suffix = entityId ? `?entity_id=${encodeURIComponent(entityId)}` : "";
    return get<AttackTimeline[]>(`/threat-intel/attack-timeline${suffix}`);
  },
  async heatmap() {
    return get<{ country: string; tactic: string; count: number; risk_score: number }[]>("/threat-intel/risk-heatmap");
  },
  async graph(limit = 80) {
    return get<ThreatGraph>(`/threat-intel/threat-graph?limit=${limit}`);
  },
  async rules() {
    return get<DetectionRuleSummary[]>("/detections/rules");
  },
  async loadedRules() {
    return get<LoadedDetectionRule[]>("/detections/loaded-rules");
  },
  async ruleStats() {
    return get<{ total_rule_hits: number; rules: DetectionRuleSummary[] }>("/detections/rule-stats");
  },
  async search(query: string) {
    if (!query.trim()) return [];
    return get<Record<string, unknown>[]>(`/threat-intel/search?q=${encodeURIComponent(query)}`);
  },
};

async function get<T>(url: string): Promise<T> {
  return (await httpClient.get<T>(url)).data;
}

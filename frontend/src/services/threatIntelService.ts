import { httpClient, rethrowAuthError } from "../api/httpClient";
import type { AttackTimeline, DetectionRuleSummary, EntityProfile, IOCMatch, MitreTechnique, ThreatGraph, ThreatOverview } from "../types/threatIntel";

export const threatIntelService = {
  async overview() {
    return getOrFallback<ThreatOverview>("/threat-intel/overview", emptyOverview());
  },
  async iocs(limit = 100) {
    return getOrFallback<IOCMatch[]>(`/threat-intel/iocs?limit=${limit}`, []);
  },
  async entities(limit = 100) {
    return getOrFallback<EntityProfile[]>(`/threat-intel/entities?limit=${limit}`, []);
  },
  async highRiskEntities(limit = 50) {
    return getOrFallback<EntityProfile[]>(`/entities/high-risk?limit=${limit}`, []);
  },
  async entity(entityId: string) {
    return getOrFallback<EntityProfile | null>(`/entities/${encodeURIComponent(entityId)}`, null);
  },
  async mitre() {
    return getOrFallback<MitreTechnique[]>("/threat-intel/mitre", []);
  },
  async tactics() {
    return getOrFallback<string[]>("/mitre/tactics", []);
  },
  async techniques() {
    return getOrFallback<MitreTechnique[]>("/mitre/techniques", []);
  },
  async attackTimeline(entityId?: string) {
    const suffix = entityId ? `?entity_id=${encodeURIComponent(entityId)}` : "";
    return getOrFallback<AttackTimeline[]>(`/threat-intel/attack-timeline${suffix}`, []);
  },
  async heatmap() {
    return getOrFallback<{ country: string; tactic: string; count: number; risk_score: number }[]>("/threat-intel/risk-heatmap", []);
  },
  async graph(limit = 80) {
    return getOrFallback<ThreatGraph>(`/threat-intel/threat-graph?limit=${limit}`, { nodes: [], edges: [] });
  },
  async rules() {
    return getOrFallback<DetectionRuleSummary[]>("/detections/rules", []);
  },
  async ruleStats() {
    return getOrFallback<{ total_rule_hits: number; rules: DetectionRuleSummary[] }>("/detections/rule-stats", { total_rule_hits: 0, rules: [] });
  },
  async search(query: string) {
    if (!query.trim()) return [];
    return getOrFallback<Record<string, unknown>[]>(`/threat-intel/search?q=${encodeURIComponent(query)}`, []);
  },
};

function emptyOverview(): ThreatOverview {
  return { generated_at: new Date().toISOString(), total_enriched_events: 0, ioc_matches: 0, high_risk_entities: 0, rule_hits: 0, active_timelines: 0, severity_counts: {} };
}

async function getOrFallback<T>(url: string, fallback: T): Promise<T> {
  try {
    const response = await httpClient.get<T>(url);
    return response.data;
  } catch (error) {
    rethrowAuthError(error);
    return fallback;
  }
}

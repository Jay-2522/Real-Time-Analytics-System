import { httpClient, rethrowAuthError } from "../api/httpClient";
import type { AnalyticsRecord, ChartPoint, DashboardOverview, EntityRiskPoint, SystemHealth } from "../types/dashboard";

const fallbackOverview: DashboardOverview = {
  total_events: 0,
  total_predictions: 0,
  anomaly_count: 0,
  high_risk_count: 0,
  average_risk_score: 0,
  system_status: "waiting_for_streams",
  generated_at: new Date().toISOString(),
};

const fallbackTrend: ChartPoint[] = [
  { label: "00:00", value: 22, secondary_value: 4 },
  { label: "00:05", value: 38, secondary_value: 7 },
  { label: "00:10", value: 46, secondary_value: 9 },
  { label: "00:15", value: 31, secondary_value: 5 },
  { label: "00:20", value: 64, secondary_value: 13 },
];

export const dashboardService = {
  async overview() {
    return getOrFallback<DashboardOverview>("/dashboard/overview", fallbackOverview);
  },
  async systemHealth() {
    return getOrFallback<SystemHealth>("/dashboard/system-health", {
      status: "degraded",
      generated_at: new Date().toISOString(),
      components: [{ name: "Backend", status: "checking", detail: "Waiting for backend response." }],
    });
  },
  async riskTrends() {
    return nonEmpty(await getOrFallback<ChartPoint[]>("/dashboard/risk-trends", []), fallbackTrend);
  },
  async eventVolume() {
    return nonEmpty(await getOrFallback<ChartPoint[]>("/dashboard/event-volume", []), fallbackTrend);
  },
  async severityDistribution() {
    return nonEmpty(await getOrFallback<ChartPoint[]>("/dashboard/severity-distribution", []), [
      { label: "low", value: 42 },
      { label: "medium", value: 18 },
      { label: "high", value: 9 },
      { label: "critical", value: 3 },
    ]);
  },
  async eventTypeDistribution() {
    return nonEmpty(await getOrFallback<ChartPoint[]>("/dashboard/event-type-distribution", []), [
      { label: "login", value: 34 },
      { label: "api", value: 29 },
      { label: "network", value: 21 },
      { label: "anomaly", value: 8 },
    ]);
  },
  async topEntities() {
    return nonEmpty(await getOrFallback<EntityRiskPoint[]>("/dashboard/top-entities", []), [
      { entity_id: "10.12.4.91", risk_score: 91, event_count: 14, severity: "critical" },
      { entity_id: "u-1007", risk_score: 84, event_count: 9, severity: "high" },
      { entity_id: "/api/v1/admin", risk_score: 76, event_count: 7, severity: "high" },
    ]);
  },
  async recentEvents(limit = 50) {
    return getOrFallback<AnalyticsRecord[]>(`/events/recent?limit=${limit}`, []);
  },
  async recentPredictions(limit = 50) {
    return getOrFallback<AnalyticsRecord[]>(`/predictions/recent?limit=${limit}`, []);
  },
  async highRisks(limit = 25) {
    return getOrFallback<AnalyticsRecord[]>(`/risks/high?limit=${limit}`, []);
  },
  async searchEvents(query: string, limit = 25) {
    if (!query.trim()) return [];
    return getOrFallback<Record<string, unknown>[]>(`/search/events?q=${encodeURIComponent(query)}&limit=${limit}`, []);
  },
};

async function getOrFallback<T>(url: string, fallback: T): Promise<T> {
  try {
    const response = await httpClient.get<T>(url);
    return response.data;
  } catch (error) {
    rethrowAuthError(error);
    return fallback;
  }
}

function nonEmpty<T>(value: T[], fallback: T[]): T[] {
  return value.length ? value : fallback;
}


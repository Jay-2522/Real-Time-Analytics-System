import { httpClient } from "../api/httpClient";
import type { AnalyticsRecord, ChartPoint, DashboardOverview, EntityRiskPoint, SystemHealth } from "../types/dashboard";

export const dashboardService = {
  async overview() {
    return get<DashboardOverview>("/dashboard/overview");
  },
  async systemHealth() {
    return get<SystemHealth>("/dashboard/system-health");
  },
  async riskTrends() {
    return get<ChartPoint[]>("/dashboard/risk-trends");
  },
  async eventVolume() {
    return get<ChartPoint[]>("/dashboard/event-volume");
  },
  async severityDistribution() {
    return get<ChartPoint[]>("/dashboard/severity-distribution");
  },
  async eventTypeDistribution() {
    return get<ChartPoint[]>("/dashboard/event-type-distribution");
  },
  async topEntities() {
    return get<EntityRiskPoint[]>("/dashboard/top-entities");
  },
  async recentEvents(limit = 50) {
    return get<AnalyticsRecord[]>(`/events/recent?limit=${limit}`);
  },
  async recentPredictions(limit = 50) {
    return get<AnalyticsRecord[]>(`/predictions/recent?limit=${limit}`);
  },
  async highRisks(limit = 25) {
    return get<AnalyticsRecord[]>(`/risks/high?limit=${limit}`);
  },
  async searchEvents(query: string, limit = 25) {
    if (!query.trim()) return [];
    return get<Record<string, unknown>[]>(`/search/events?q=${encodeURIComponent(query)}&limit=${limit}`);
  },
};

async function get<T>(url: string): Promise<T> {
  return (await httpClient.get<T>(url)).data;
}


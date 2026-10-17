import { httpClient, rethrowAuthError } from "../api/httpClient";
import type { AlertRecord, AlertStats, IncidentRecord } from "../types/alerts";

const fallbackStats: AlertStats = {
  total_alerts: 0,
  open_alerts: 0,
  critical_alerts: 0,
  high_alerts: 0,
  total_incidents: 0,
  open_incidents: 0,
  severity_counts: {},
};

export const alertService = {
  async recentAlerts(limit = 50) {
    return getOrFallback<AlertRecord[]>(`/alerts/recent?limit=${limit}`, []);
  },
  async highAlerts(limit = 50) {
    return getOrFallback<AlertRecord[]>(`/alerts/high?limit=${limit}`, []);
  },
  async criticalAlerts(limit = 25) {
    return getOrFallback<AlertRecord[]>(`/alerts/critical?limit=${limit}`, []);
  },
  async recentIncidents(limit = 50) {
    return getOrFallback<IncidentRecord[]>(`/incidents/recent?limit=${limit}`, []);
  },
  async openIncidents(limit = 50) {
    return getOrFallback<IncidentRecord[]>(`/incidents/open?limit=${limit}`, []);
  },
  async stats() {
    return getOrFallback<AlertStats>("/alerts/stats", fallbackStats);
  },
  async search(query: string, limit = 25) {
    if (!query.trim()) return [];
    return getOrFallback<Record<string, unknown>[]>(`/alerts/search?q=${encodeURIComponent(query)}&limit=${limit}`, []);
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


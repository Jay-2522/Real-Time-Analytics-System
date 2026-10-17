import { httpClient, rethrowAuthError } from "../api/httpClient";
import type { ErrorSummary, MetricsSummary, MonitoringOverview, PipelineMetrics, ServiceHealth } from "../types/monitoring";

export const monitoringService = {
  async overview() {
    return getOrFallback<MonitoringOverview>("/monitoring/overview", {
      status: "degraded",
      services_total: 0,
      services_healthy: 0,
      services_degraded: 0,
      services_down: 0,
      total_events: 0,
      total_predictions: 0,
      total_alerts: 0,
      open_incidents: 0,
      generated_at: new Date().toISOString(),
      prometheus_url: "http://localhost:9090",
      grafana_url: "http://localhost:3000",
    });
  },
  async services() {
    return getOrFallback<ServiceHealth[]>("/monitoring/services", []);
  },
  async pipeline() {
    return getOrFallback<PipelineMetrics>("/monitoring/pipeline", {
      telemetry_events: 0,
      analytics_metrics: 0,
      risk_metrics: 0,
      feature_snapshots: 0,
      anomaly_predictions: 0,
      alerts: 0,
      incidents: 0,
      deadletter_events: 0,
      generated_at: new Date().toISOString(),
    });
  },
  async errors() {
    return getOrFallback<ErrorSummary>("/monitoring/errors", {
      deadletter_events: 0,
      critical_alerts: 0,
      failed_services: 0,
      degraded_services: 0,
      recent_errors: [],
      generated_at: new Date().toISOString(),
    });
  },
  async metricsSummary() {
    return getOrFallback<MetricsSummary>("/monitoring/metrics-summary", {
      prometheus_available: false,
      scraped_targets_up: 0,
      scraped_targets_down: 0,
      key_metrics: {},
      generated_at: new Date().toISOString(),
    });
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

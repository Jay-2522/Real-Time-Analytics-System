import { httpClient } from "../api/httpClient";
import type { ErrorSummary, MetricsSummary, MonitoringOverview, PipelineMetrics, ServiceHealth } from "../types/monitoring";

export const monitoringService = {
  async overview() {
    return get<MonitoringOverview>("/monitoring/overview");
  },
  async services() {
    return get<ServiceHealth[]>("/monitoring/services");
  },
  async pipeline() {
    return get<PipelineMetrics>("/monitoring/pipeline");
  },
  async errors() {
    return get<ErrorSummary>("/monitoring/errors");
  },
  async metricsSummary() {
    return get<MetricsSummary>("/monitoring/metrics-summary");
  },
};

async function get<T>(url: string): Promise<T> {
  return (await httpClient.get<T>(url)).data;
}

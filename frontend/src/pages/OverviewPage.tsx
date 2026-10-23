import { Activity, AlertTriangle, BrainCircuit, Gauge, ShieldAlert, Signal } from "lucide-react";

import { EventVolumeChart, RiskTrendChart, SeverityDistributionChart, TopEntitiesChart } from "../components/charts/DashboardCharts";
import { MetricCard } from "../components/dashboard/MetricCard";
import { RecordsTable } from "../components/tables/RecordTables";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { useDashboardWebSocket } from "../hooks/useDashboardWebSocket";
import { dashboardService } from "../services/dashboardService";

export function OverviewPage() {
  const overview = useAsyncData(dashboardService.overview, [], 10_000);
  const riskTrends = useAsyncData(dashboardService.riskTrends, [], 15_000);
  const eventVolume = useAsyncData(dashboardService.eventVolume, [], 15_000);
  const severity = useAsyncData(dashboardService.severityDistribution, [], 15_000);
  const topEntities = useAsyncData(dashboardService.topEntities, [], 15_000);
  const recent = useAsyncData(() => dashboardService.recentEvents(8), [], 10_000);
  const live = useDashboardWebSocket();
  const data = live.message?.overview ?? overview.data;

  return (
    <div className="space-y-6">
      <ApiError errors={[overview.error, riskTrends.error, eventVolume.error, severity.error, topEntities.error, recent.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-sm font-medium text-cyan-200">AI Streaming Analytics</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Real-time security telemetry command center</h1>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">
              Live Kafka telemetry, Spark analytics, ML anomaly scoring, searchable persistence, and system health in one local enterprise dashboard.
            </p>
          </div>
          <div className="rounded-md border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-300">
            Live channel: <span className="font-medium text-cyan-200">{live.status}</span>
          </div>
        </div>
      </section>

      {data ? <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-6">
        <MetricCard label="Total Events" value={data?.total_events ?? "..."} detail="Persisted telemetry and analytics records" icon={Activity} />
        <MetricCard label="Predictions" value={data?.total_predictions ?? "..."} detail="ML inference records" icon={BrainCircuit} tone="emerald" />
        <MetricCard label="Anomalies" value={data?.anomaly_count ?? "..."} detail="Isolation Forest anomaly decisions" icon={ShieldAlert} tone="amber" />
        <MetricCard label="High Risk" value={data?.high_risk_count ?? "..."} detail="Risk score above operating threshold" icon={AlertTriangle} tone="red" />
        <MetricCard label="Avg Risk" value={data?.average_risk_score ?? "..."} detail="Mean ML risk score" icon={Gauge} tone="cyan" />
        <MetricCard label="System" value={data?.system_status ?? "checking"} detail="Local platform operating state" icon={Signal} tone="emerald" />
      </section> : <EmptyState />}

      <section className="grid gap-4 xl:grid-cols-2">
        <RiskTrendChart data={riskTrends.data ?? []} />
        <EventVolumeChart data={eventVolume.data ?? []} />
        <SeverityDistributionChart data={severity.data ?? []} />
        <TopEntitiesChart data={topEntities.data ?? []} />
      </section>

      <RecordsTable title="Recent Streaming Events" records={recent.data ?? []} />
    </div>
  );
}


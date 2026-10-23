import { EventVolumeChart, RiskTrendChart, SeverityDistributionChart, TopEntitiesChart } from "../components/charts/DashboardCharts";
import { TopEntitiesTable } from "../components/tables/RecordTables";
import { ApiError } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { dashboardService } from "../services/dashboardService";

export function RiskAnalyticsPage() {
  const riskTrends = useAsyncData(dashboardService.riskTrends, [], 10_000);
  const eventVolume = useAsyncData(dashboardService.eventVolume, [], 10_000);
  const severity = useAsyncData(dashboardService.severityDistribution, [], 10_000);
  const topEntities = useAsyncData(dashboardService.topEntities, [], 10_000);

  return (
    <div className="space-y-6">
      <ApiError errors={[riskTrends.error, eventVolume.error, severity.error, topEntities.error]} />
      <Header title="Risk Analytics" detail="Streaming risk posture, severity mix, event volume, and top risky entities." />
      <section className="grid gap-4 xl:grid-cols-2">
        <RiskTrendChart data={riskTrends.data ?? []} />
        <EventVolumeChart data={eventVolume.data ?? []} />
        <SeverityDistributionChart data={severity.data ?? []} />
        <TopEntitiesChart data={topEntities.data ?? []} />
      </section>
      <TopEntitiesTable entities={topEntities.data ?? []} />
    </div>
  );
}

function Header({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
      <h1 className="text-2xl font-semibold text-white">{title}</h1>
      <p className="mt-2 text-sm text-slate-400">{detail}</p>
    </section>
  );
}


import { Users } from "lucide-react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { SeverityBadge } from "../components/ui/SeverityBadge";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { threatIntelService } from "../services/threatIntelService";

export function EntitiesPage() {
  const entities = useAsyncData(() => threatIntelService.entities(50), [], 10_000);
  const selected = entities.data?.[0];
  return (
    <div className="space-y-6">
      <ApiError errors={[entities.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">Entity Intelligence</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Risk Profiles</h1>
        <p className="mt-2 text-sm text-slate-400">Entity baselines, risk trends, anomaly history, and ATT&CK relationships.</p>
      </section>
      <section className="grid gap-4 xl:grid-cols-[.85fr_1.15fr]">
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="flex items-center gap-2 font-semibold text-white"><Users size={18} /> High Risk Entities</h2>
          <div className="mt-4 space-y-3">
            {(entities.data ?? []).map((entity) => (
              <div key={entity.entity_id} className="rounded-md border border-white/10 bg-white/[0.03] p-4">
                <div className="flex items-center justify-between gap-3">
                  <span className="font-mono text-sm text-white">{entity.entity_id}</span>
                  <SeverityBadge severity={entity.risk_score >= 90 ? "critical" : entity.risk_score >= 75 ? "high" : "medium"} />
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-xs text-slate-400">
                  <span>{entity.entity_type}</span>
                  <span>{entity.alert_count} alerts</span>
                  <span>{entity.ioc_count} IOCs</span>
                </div>
              </div>
            ))}
            {entities.data && !entities.data.length ? <EmptyState /> : null}
          </div>
        </article>
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="font-semibold text-white">Behavioral Baseline</h2>
          {selected ? (
            <>
              <p className="mt-2 font-mono text-sm text-cyan-200">{selected.entity_id}</p>
              <div className="mt-4 h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={selected.risk_trend}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,.15)" />
                    <XAxis dataKey="ts" hide />
                    <YAxis stroke="#94a3b8" />
                    <Tooltip />
                    <Area type="monotone" dataKey="score" stroke="#22d3ee" fill="#22d3ee33" />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <div className="mt-4 flex flex-wrap gap-2">{selected.tactics.map((tactic) => <span key={tactic} className="rounded-full bg-cyan-400/10 px-3 py-1 text-xs text-cyan-100">{tactic}</span>)}</div>
            </>
          ) : <EmptyState className="mt-4" />}
        </article>
      </section>
    </div>
  );
}

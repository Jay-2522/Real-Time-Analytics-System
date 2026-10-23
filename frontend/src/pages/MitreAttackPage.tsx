import { Crosshair } from "lucide-react";

import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { threatIntelService } from "../services/threatIntelService";

export function MitreAttackPage() {
  const tactics = useAsyncData(threatIntelService.tactics, [], 30_000);
  const active = useAsyncData(threatIntelService.mitre, [], 10_000);
  const activeByTactic = new Map((active.data ?? []).map((item) => [item.tactic, item]));
  return (
    <div className="space-y-6">
      <ApiError errors={[tactics.error, active.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">ATT&CK Coverage</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">MITRE ATT&CK Matrix</h1>
        <p className="mt-2 text-sm text-slate-400">Technique frequency and active attack-chain mapping from enriched telemetry.</p>
      </section>
      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {(tactics.data ?? []).map((tactic) => {
          const item = activeByTactic.get(tactic);
          const hot = Number(item?.count ?? 0) > 0;
          return (
            <article key={tactic} className={`rounded-lg border p-4 shadow-panel ${hot ? "border-cyan-400/30 bg-cyan-400/10" : "border-white/10 bg-surface-900"}`}>
              <div className="flex items-start justify-between gap-3">
                <h2 className="font-semibold text-white">{tactic}</h2>
                <Crosshair size={18} className={hot ? "text-cyan-200" : "text-slate-600"} />
              </div>
              <p className="mt-4 text-sm text-slate-400">{item?.technique ?? "No active mapped technique"}</p>
              <p className="mt-3 font-mono text-xs text-slate-500">{item?.mitre_id ?? "waiting for signal"}</p>
              <div className="mt-4 h-2 rounded-full bg-white/10">
                <div className="h-2 rounded-full bg-cyan-300" style={{ width: `${Math.min(100, Number(item?.avg_threat_score ?? 0))}%` }} />
              </div>
            </article>
          );
        })}
      </section>
      {tactics.data && !tactics.data.length ? <EmptyState /> : null}
    </div>
  );
}

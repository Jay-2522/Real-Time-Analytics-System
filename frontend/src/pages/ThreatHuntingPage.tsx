import { useState } from "react";
import { Search, Target } from "lucide-react";

import { SeverityBadge } from "../components/ui/SeverityBadge";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { threatIntelService } from "../services/threatIntelService";

export function ThreatHuntingPage() {
  const [query, setQuery] = useState("credential OR lateral OR T1110");
  const results = useAsyncData(() => threatIntelService.search(query), [query], 0);
  const iocs = useAsyncData(() => threatIntelService.iocs(12), [], 10_000);
  return (
    <div className="space-y-6">
      <ApiError errors={[results.error, iocs.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">Investigation Workbench</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Threat Hunting</h1>
        <p className="mt-2 text-sm text-slate-400">Pivot across IOCs, entities, MITRE techniques, and enriched security telemetry.</p>
      </section>
      <section className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
        <div className="flex items-center gap-3 rounded-md border border-cyan-400/20 bg-surface-950 px-4 py-3">
          <Search size={18} className="text-cyan-200" />
          <input value={query} onChange={(event) => setQuery(event.target.value)} className="w-full bg-transparent text-sm text-white outline-none" />
        </div>
        <div className="mt-4 grid gap-3 xl:grid-cols-[1fr_.45fr]">
          <div className="rounded-md border border-white/10">
            {(results.data ?? []).map((row, index) => (
              <div key={index} className="border-b border-white/10 p-4 last:border-b-0">
                <p className="font-mono text-xs text-cyan-200">{String(row.entity_id ?? row.ioc_value ?? row.event_id ?? "hunt-result")}</p>
                <p className="mt-2 text-sm text-slate-300">{String(row.description ?? row.explanation ?? row.tactic ?? row.technique ?? "Threat intelligence match")}</p>
              </div>
            ))}
            {results.data && !results.data.length ? <EmptyState className="p-4" /> : null}
          </div>
          <aside className="rounded-md border border-white/10 p-4">
            <h2 className="flex items-center gap-2 font-semibold text-white"><Target size={18} /> IOC Pivots</h2>
            <div className="mt-3 space-y-2">
              {(iocs.data ?? []).map((ioc) => (
                <button key={`${ioc.id}-${ioc.ioc_value}`} onClick={() => setQuery(ioc.ioc_value)} className="flex w-full items-center justify-between gap-3 rounded-md bg-white/5 px-3 py-2 text-left text-sm">
                  <span className="truncate text-slate-200">{ioc.ioc_value}</span>
                  <SeverityBadge severity={ioc.severity} />
                </button>
              ))}
              {iocs.data && !iocs.data.length ? <EmptyState /> : null}
            </div>
          </aside>
        </div>
      </section>
    </div>
  );
}

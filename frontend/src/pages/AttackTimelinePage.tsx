import { Clock3 } from "lucide-react";

import { ThreatGraphPanel } from "../components/threat/ThreatGraphPanel";
import { SeverityBadge } from "../components/ui/SeverityBadge";
import { ApiError, EmptyState } from "../components/ui/DataState";
import { useAsyncData } from "../hooks/useAsyncData";
import { threatIntelService } from "../services/threatIntelService";

export function AttackTimelinePage() {
  const timelines = useAsyncData(() => threatIntelService.attackTimeline(), [], 10_000);
  const active = timelines.data?.[0];
  return (
    <div className="space-y-6">
      <ApiError errors={[timelines.error]} />
      <section className="rounded-lg border border-white/10 bg-surface-900 p-6 shadow-panel">
        <p className="text-sm font-medium text-cyan-200">Attack Chain Reconstruction</p>
        <h1 className="mt-2 text-2xl font-semibold text-white">Attack Timeline</h1>
        <p className="mt-2 text-sm text-slate-400">Chronological incident progression, severity escalation, and entity relationships.</p>
      </section>
      <section className="grid gap-4 xl:grid-cols-[.8fr_1.2fr]">
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="flex items-center gap-2 font-semibold text-white"><Clock3 size={18} /> Timelines</h2>
          <div className="mt-4 space-y-3">
            {(timelines.data ?? []).map((timeline) => (
              <div key={timeline.timeline_id} className="rounded-md border border-white/10 bg-white/[0.03] p-4">
                <div className="flex items-center justify-between gap-3">
                  <span className="font-mono text-sm text-white">{timeline.entity_id}</span>
                  <SeverityBadge severity={timeline.severity} />
                </div>
                <p className="mt-2 text-xs text-slate-500">{timeline.event_count} events · updated {new Date(timeline.updated_at).toLocaleString()}</p>
              </div>
            ))}
            {timelines.data && !timelines.data.length ? <EmptyState /> : null}
          </div>
        </article>
        <article className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
          <h2 className="font-semibold text-white">Attack Path Visualization</h2>
          {active ? <div className="mt-4"><ThreatGraphPanel graph={{ nodes: active.nodes.map((node) => ({ ...node, risk_score: node.score })), edges: active.edges }} /></div> : <EmptyState className="mt-4" />}
        </article>
      </section>
    </div>
  );
}

import { Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { ChartPoint, EntityRiskPoint } from "../../types/dashboard";
import { EmptyState } from "../ui/DataState";

const colors = ["#22d3ee", "#34d399", "#f59e0b", "#f97316", "#ef4444"];

function ChartFrame({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-lg border border-white/10 bg-surface-900 p-5 shadow-panel">
      <h2 className="text-sm font-semibold text-white">{title}</h2>
      <div className="mt-4 h-72">{children}</div>
    </section>
  );
}

export function RiskTrendChart({ data }: { data: ChartPoint[] }) {
  if (!data.length) return <EmptyChart title="Risk Trend" />;
  return (
    <ChartFrame title="Risk Trend">
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="#243145" strokeDasharray="3 3" />
          <XAxis dataKey="label" stroke="#94a3b8" />
          <YAxis stroke="#94a3b8" />
          <Tooltip contentStyle={{ background: "#0d1728", border: "1px solid rgba(255,255,255,.12)", color: "#fff" }} />
          <Line type="monotone" dataKey="value" stroke="#22d3ee" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}

export function EventVolumeChart({ data }: { data: ChartPoint[] }) {
  if (!data.length) return <EmptyChart title="Event Volume" />;
  return (
    <ChartFrame title="Event Volume">
      <ResponsiveContainer>
        <AreaChart data={data}>
          <CartesianGrid stroke="#243145" strokeDasharray="3 3" />
          <XAxis dataKey="label" stroke="#94a3b8" />
          <YAxis stroke="#94a3b8" />
          <Tooltip contentStyle={{ background: "#0d1728", border: "1px solid rgba(255,255,255,.12)", color: "#fff" }} />
          <Area type="monotone" dataKey="value" stroke="#34d399" fill="#34d399" fillOpacity={0.18} />
        </AreaChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}

export function SeverityDistributionChart({ data }: { data: ChartPoint[] }) {
  if (!data.length) return <EmptyChart title="Severity Distribution" />;
  return (
    <ChartFrame title="Severity Distribution">
      <ResponsiveContainer>
        <PieChart>
          <Pie data={data} dataKey="value" nameKey="label" innerRadius={58} outerRadius={92} paddingAngle={4}>
            {data.map((entry, index) => (
              <Cell key={entry.label} fill={colors[index % colors.length]} />
            ))}
          </Pie>
          <Tooltip contentStyle={{ background: "#0d1728", border: "1px solid rgba(255,255,255,.12)", color: "#fff" }} />
        </PieChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}

export function TopEntitiesChart({ data }: { data: EntityRiskPoint[] }) {
  if (!data.length) return <EmptyChart title="Top Risky Entities" />;
  return (
    <ChartFrame title="Top Risky Entities">
      <ResponsiveContainer>
        <BarChart data={data}>
          <CartesianGrid stroke="#243145" strokeDasharray="3 3" />
          <XAxis dataKey="entity_id" stroke="#94a3b8" hide />
          <YAxis stroke="#94a3b8" />
          <Tooltip contentStyle={{ background: "#0d1728", border: "1px solid rgba(255,255,255,.12)", color: "#fff" }} />
          <Bar dataKey="risk_score" fill="#f59e0b" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}

function EmptyChart({ title }: { title: string }) {
  return <ChartFrame title={title}><div className="grid h-full place-items-center"><EmptyState /></div></ChartFrame>;
}


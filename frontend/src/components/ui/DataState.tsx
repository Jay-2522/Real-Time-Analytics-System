export function ApiError({ errors }: { errors: Array<string | null> }) {
  if (!errors.some(Boolean)) return null;
  return <div role="alert" className="rounded-md border border-red-400/30 bg-red-400/10 px-4 py-3 text-sm text-red-100">API unreachable</div>;
}

export function EmptyState({ className = "" }: { className?: string }) {
  return <p className={`text-sm text-slate-400 ${className}`}>No data yet</p>;
}

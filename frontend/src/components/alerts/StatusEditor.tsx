import { useEffect, useState } from "react";
import { useSession } from "../../auth/session";
import { httpClient } from "../../api/httpClient";

export function StatusEditor({ kind, id, status }: { kind: "alerts" | "incidents"; id: string; status: string }) {
  const { user } = useSession();
  const [value, setValue] = useState(status);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  useEffect(() => setValue(status), [status]);
  if (user.role === "viewer") return <span>{status}</span>;
  async function update(next: string) {
    setBusy(true); setMessage("");
    try {
      const { data } = await httpClient.patch(`/${kind}/${encodeURIComponent(id)}/status`, { status: next });
      setValue(data.status); setMessage("Status saved.");
    } catch { setMessage("Status was not saved."); }
    finally { setBusy(false); }
  }
  return <div><select aria-label={`${kind} ${id} status`} value={value} disabled={busy} onChange={(event) => void update(event.target.value)} className="rounded bg-surface-900 p-2 text-white">
    {["open", "investigating", "resolved", "acknowledged"].map((item) => <option key={item} value={item}>{item}</option>)}
  </select>{message && <p role="status" className="text-xs">{message}</p>}</div>;
}

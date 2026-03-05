import type { Incident } from '@/lib/api';

export function Timeline({ incident }: { incident: Incident }) {
  return (
    <section className="card">
      <h2 className="text-lg font-semibold">Timeline</h2>
      <div className="mt-4 space-y-3">
        {incident.timeline.length === 0 ? (
          <p className="text-sm text-muted">No timeline events yet.</p>
        ) : (
          incident.timeline.map((entry, idx) => (
            <div key={`${entry.timestamp}-${idx}`} className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
              <p className="text-xs text-muted">{new Date(entry.timestamp).toLocaleString()}</p>
              <p className="mt-1 text-sm text-slate-200">{entry.message}</p>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

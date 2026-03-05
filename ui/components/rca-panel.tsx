import type { Incident } from '@/lib/api';

export function RCAPanel({ incident }: { incident: Incident }) {
  if (!incident.rca) {
    return (
      <section className="card">
        <h2 className="text-lg font-semibold">Root Cause Analysis</h2>
        <p className="mt-3 text-sm text-muted">RCA is pending generation.</p>
      </section>
    );
  }

  return (
    <section className="card">
      <h2 className="text-lg font-semibold">Root Cause Analysis</h2>
      <p className="mt-2 text-sm text-slate-200">{incident.rca.root_cause}</p>
      <p className="mt-2 text-xs text-muted">Confidence: {(incident.rca.confidence * 100).toFixed(0)}%</p>
      <p className="mt-4 text-sm text-slate-300">{incident.rca.explanation}</p>
      <p className="mt-4 rounded-lg border border-accent/30 bg-accent/10 p-3 text-sm text-slate-200">
        Suggested fix: {incident.rca.suggested_fix}
      </p>
      <div className="mt-4 flex flex-wrap gap-2">
        {incident.rca.affected_services.map((service) => (
          <span key={service} className="rounded-md bg-slate-800 px-2 py-1 text-xs text-slate-300">
            {service}
          </span>
        ))}
      </div>
    </section>
  );
}

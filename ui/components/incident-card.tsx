import Link from 'next/link';

import type { Incident } from '@/lib/api';
import { StatusBadge } from './status-badge';

export function IncidentCard({ incident }: { incident: Incident }) {
  return (
    <Link href={`/incidents/${incident.id}`} className="card block transition hover:-translate-y-0.5 hover:border-accent/50">
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-slate-100">{incident.service}</p>
        <div className="flex gap-2">
          <StatusBadge kind="severity" value={incident.severity} />
          <StatusBadge kind="status" value={incident.status} />
        </div>
      </div>
      <p className="mt-3 text-sm text-slate-300">{incident.summary}</p>
      <p className="mt-4 text-xs text-muted">{new Date(incident.created_at).toLocaleString()}</p>
    </Link>
  );
}

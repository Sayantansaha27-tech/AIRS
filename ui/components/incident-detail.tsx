'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';

import { fetchIncident, type Incident } from '@/lib/api';
import { LogViewer } from './log-viewer';
import { RCAPanel } from './rca-panel';
import { StatusBadge } from './status-badge';
import { Timeline } from './timeline';
import { TopNav } from './top-nav';

const POLL_INTERVAL_MS = 5000;

export function IncidentDetail({ incidentId }: { incidentId: string }) {
  const [incident, setIncident] = useState<Incident | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;

    const load = async () => {
      try {
        const next = await fetchIncident(incidentId);
        if (!alive) return;
        setIncident(next);
        setError(null);
      } catch (err) {
        if (!alive) return;
        setError((err as Error).message);
      }
    };

    load();
    const timer = setInterval(load, POLL_INTERVAL_MS);

    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [incidentId]);

  return (
    <div className="mx-auto max-w-6xl px-6 py-10">
      <TopNav />

      <div className="mb-6 flex items-center justify-between gap-3">
        <Link href="/" className="text-sm text-accent hover:underline">
          ← Back to dashboard
        </Link>
        {incident && (
          <div className="flex gap-2">
            <StatusBadge kind="severity" value={incident.severity} />
            <StatusBadge kind="status" value={incident.status} />
          </div>
        )}
      </div>

      {error && <p className="rounded-lg border border-critical/40 bg-critical/10 p-3 text-sm text-critical">{error}</p>}

      {!incident ? (
        <div className="card">
          <p className="text-sm text-muted">Loading incident...</p>
        </div>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          <div className="space-y-4 lg:col-span-2">
            <RCAPanel incident={incident} />
          </div>
          <Timeline incident={incident} />
          <LogViewer incident={incident} />
        </div>
      )}
    </div>
  );
}

'use client';

import { useEffect, useState } from 'react';

import { fetchIncidents, type Incident } from '@/lib/api';
import { IncidentCard } from './incident-card';
import { TopNav } from './top-nav';

const POLL_INTERVAL_MS = 5000;

export function Dashboard() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;

    const load = async () => {
      try {
        const next = await fetchIncidents();
        if (!alive) return;
        setIncidents(next);
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
  }, []);

  return (
    <div className="mx-auto max-w-6xl px-6 py-10">
      <TopNav />

      <header className="mb-8">
        <h1 className="text-3xl font-semibold tracking-tight">AIRS Incident Console</h1>
        <p className="mt-2 text-sm text-muted">Live incident feed (polling every 5 seconds).</p>
      </header>

      {error && <p className="mb-6 rounded-lg border border-critical/40 bg-critical/10 p-3 text-sm text-critical">{error}</p>}

      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {incidents.length === 0 ? (
          <div className="card sm:col-span-2 lg:col-span-3">
            <p className="text-sm text-muted">No incidents available.</p>
          </div>
        ) : (
          incidents.map((incident) => <IncidentCard key={incident.id} incident={incident} />)
        )}
      </section>
    </div>
  );
}

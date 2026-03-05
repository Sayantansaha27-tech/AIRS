'use client';

import { useMemo, useState } from 'react';

import type { Incident } from '@/lib/api';

export function LogViewer({ incident }: { incident: Incident }) {
  const [query, setQuery] = useState('');

  const rows = useMemo(() => {
    if (!query.trim()) {
      return incident.timeline;
    }
    const lower = query.toLowerCase();
    return incident.timeline.filter((row) => row.message.toLowerCase().includes(lower));
  }, [incident.timeline, query]);

  return (
    <section className="card">
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-lg font-semibold">Log Viewer</h2>
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search logs"
          className="w-44 rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-xs text-slate-200 outline-none focus:border-accent"
        />
      </div>
      <div className="mt-4 max-h-72 overflow-auto rounded-lg border border-slate-800 bg-black/30 p-2">
        {rows.length === 0 ? (
          <p className="p-3 text-xs text-muted">No matching log lines.</p>
        ) : (
          rows.map((row, idx) => (
            <div key={`${row.timestamp}-${idx}`} className="log-line">
              [{new Date(row.timestamp).toISOString()}] {row.message}
            </div>
          ))
        )}
      </div>
    </section>
  );
}

'use client';

import { FormEvent, useEffect, useMemo, useState } from 'react';

import {
  createSource,
  deleteSource,
  fetchSources,
  setSourceEnabled,
  syncSource,
  type DataSource,
  type DataSourceInput,
  type SourceMethod,
  updateSource
} from '@/lib/api';
import { TopNav } from './top-nav';

const POLL_INTERVAL_MS = 5000;

type SourceFormState = {
  name: string;
  endpoint: string;
  defaultService: string;
  method: SourceMethod;
  pollIntervalSeconds: string;
  responseLogsField: string;
  headersText: string;
  bodyText: string;
  authToken: string;
  enabled: boolean;
};

const INITIAL_FORM: SourceFormState = {
  name: '',
  endpoint: '',
  defaultService: '',
  method: 'GET',
  pollIntervalSeconds: '30',
  responseLogsField: '',
  headersText: '{\n  \n}',
  bodyText: '{\n  \n}',
  authToken: '',
  enabled: true
};

function parseJsonObject(input: string, label: string): Record<string, unknown> {
  const trimmed = input.trim();
  if (!trimmed) return {};

  let payload: unknown;
  try {
    payload = JSON.parse(trimmed);
  } catch {
    throw new Error(`${label} must be valid JSON`);
  }

  if (payload === null || Array.isArray(payload) || typeof payload !== 'object') {
    throw new Error(`${label} must be a JSON object`);
  }
  return payload as Record<string, unknown>;
}

function sourceToForm(source: DataSource): SourceFormState {
  return {
    name: source.name,
    endpoint: source.endpoint,
    defaultService: source.default_service,
    method: source.method,
    pollIntervalSeconds: String(source.poll_interval_seconds),
    responseLogsField: source.response_logs_field ?? '',
    headersText: JSON.stringify(source.headers ?? {}, null, 2),
    bodyText: JSON.stringify(source.body ?? {}, null, 2),
    authToken: '',
    enabled: source.enabled
  };
}

function buildPayload(form: SourceFormState): DataSourceInput {
  const interval = Number(form.pollIntervalSeconds);
  if (!Number.isFinite(interval) || interval < 5) {
    throw new Error('Polling interval must be at least 5 seconds');
  }

  const headersRaw = parseJsonObject(form.headersText, 'Headers');
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(headersRaw)) {
    headers[key] = String(value);
  }

  const body = parseJsonObject(form.bodyText, 'Body');
  const hasBody = Object.keys(body).length > 0;

  return {
    name: form.name.trim(),
    endpoint: form.endpoint.trim(),
    default_service: form.defaultService.trim(),
    method: form.method,
    headers,
    body: hasBody ? body : null,
    response_logs_field: form.responseLogsField.trim() || null,
    poll_interval_seconds: Math.floor(interval),
    enabled: form.enabled,
    auth_token: form.authToken.trim() || null
  };
}

function formatTimestamp(value: string | null): string {
  if (!value) return 'Never';
  const dt = new Date(value);
  if (Number.isNaN(dt.valueOf())) return 'Never';
  return dt.toLocaleString();
}

export function DataSources() {
  const [sources, setSources] = useState<DataSource[]>([]);
  const [form, setForm] = useState<SourceFormState>(INITIAL_FORM);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const sortedSources = useMemo(
    () =>
      [...sources].sort((a, b) => {
        const aDate = new Date(a.updated_at).valueOf();
        const bDate = new Date(b.updated_at).valueOf();
        return bDate - aDate;
      }),
    [sources]
  );

  useEffect(() => {
    let alive = true;

    const load = async () => {
      try {
        const next = await fetchSources();
        if (!alive) return;
        setSources(next);
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

  const onChange = (patch: Partial<SourceFormState>) => {
    setForm((prev) => ({ ...prev, ...patch }));
  };

  const resetForm = () => {
    setForm(INITIAL_FORM);
    setEditingId(null);
  };

  const refreshSources = async () => {
    const next = await fetchSources();
    setSources(next);
  };

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    try {
      setBusyKey('form');
      setError(null);
      const payload = buildPayload(form);

      if (!payload.name || !payload.endpoint || !payload.default_service) {
        throw new Error('Name, endpoint, and default service are required');
      }

      if (editingId) {
        await updateSource(editingId, payload);
        setNotice('Source updated.');
      } else {
        await createSource(payload);
        setNotice('Source created.');
      }

      await refreshSources();
      resetForm();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusyKey(null);
    }
  };

  const onEdit = (source: DataSource) => {
    setEditingId(source.id);
    setForm(sourceToForm(source));
    setNotice(`Editing ${source.name}`);
  };

  const onSync = async (source: DataSource) => {
    try {
      setBusyKey(`sync-${source.id}`);
      setError(null);
      const result = await syncSource(source.id);
      setNotice(`Synced ${source.name}: accepted ${result.accepted}/${result.fetched} logs.`);
      await refreshSources();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusyKey(null);
    }
  };

  const onToggle = async (source: DataSource) => {
    try {
      setBusyKey(`toggle-${source.id}`);
      setError(null);
      await setSourceEnabled(source.id, !source.enabled);
      setNotice(`${source.name} ${source.enabled ? 'paused' : 'enabled'}.`);
      await refreshSources();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusyKey(null);
    }
  };

  const onDelete = async (source: DataSource) => {
    try {
      setBusyKey(`delete-${source.id}`);
      setError(null);
      await deleteSource(source.id);
      if (editingId === source.id) {
        resetForm();
      }
      setNotice(`Deleted source ${source.name}.`);
      await refreshSources();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusyKey(null);
    }
  };

  return (
    <div className="mx-auto max-w-6xl px-6 py-10">
      <TopNav />

      <header className="mb-8">
        <h1 className="text-3xl font-semibold tracking-tight">External Data Sources</h1>
        <p className="mt-2 text-sm text-muted">
          Connect service APIs so AIRS can pull logs continuously and feed anomaly detection in real time.
        </p>
      </header>

      <div aria-live="polite" role="status" className="mb-4 text-sm text-slate-300">
        {notice}
      </div>
      {error && <p className="mb-6 rounded-lg border border-critical/40 bg-critical/10 p-3 text-sm text-critical">{error}</p>}

      <section className="card mb-6">
        <h2 className="text-lg font-semibold">{editingId ? 'Edit Data Source' : 'Add Data Source'}</h2>
        <p className="mt-2 text-xs text-muted">
          Use an endpoint that returns either a JSON list of logs, an object with a <code>logs</code> field, or plain text log lines.
        </p>

        <form onSubmit={onSubmit} className="mt-4 grid gap-4 md:grid-cols-2">
          <label className="text-sm text-slate-200" htmlFor="source-name">
            Source name
            <input
              id="source-name"
              value={form.name}
              onChange={(event) => onChange({ name: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              placeholder="Checkout API Logs"
              required
            />
          </label>

          <label className="text-sm text-slate-200" htmlFor="default-service">
            Default service
            <input
              id="default-service"
              value={form.defaultService}
              onChange={(event) => onChange({ defaultService: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              placeholder="checkout-service"
              required
            />
          </label>

          <label className="text-sm text-slate-200 md:col-span-2" htmlFor="endpoint">
            Endpoint URL
            <input
              id="endpoint"
              value={form.endpoint}
              onChange={(event) => onChange({ endpoint: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              placeholder="https://service.internal/logs"
              required
            />
          </label>

          <label className="text-sm text-slate-200" htmlFor="method">
            HTTP method
            <select
              id="method"
              value={form.method}
              onChange={(event) => onChange({ method: event.target.value as SourceMethod })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
            >
              <option value="GET">GET</option>
              <option value="POST">POST</option>
            </select>
          </label>

          <label className="text-sm text-slate-200" htmlFor="poll-interval">
            Poll interval (seconds)
            <input
              id="poll-interval"
              type="number"
              min={5}
              value={form.pollIntervalSeconds}
              onChange={(event) => onChange({ pollIntervalSeconds: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              required
            />
          </label>

          <label className="text-sm text-slate-200 md:col-span-2" htmlFor="response-path">
            Response logs field (optional)
            <input
              id="response-path"
              value={form.responseLogsField}
              onChange={(event) => onChange({ responseLogsField: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              placeholder="data.logs"
            />
          </label>

          <label className="text-sm text-slate-200 md:col-span-2" htmlFor="auth-token">
            Bearer token (optional)
            <input
              id="auth-token"
              type="password"
              value={form.authToken}
              onChange={(event) => onChange({ authToken: event.target.value })}
              className="mt-1 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 text-sm text-slate-100 outline-none focus:border-accent"
              placeholder={editingId ? 'Leave blank to keep existing token' : 'token'}
            />
          </label>

          <label className="text-sm text-slate-200 md:col-span-2" htmlFor="headers">
            Headers JSON
            <textarea
              id="headers"
              value={form.headersText}
              onChange={(event) => onChange({ headersText: event.target.value })}
              className="mt-1 h-28 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 font-mono text-xs text-slate-100 outline-none focus:border-accent"
              spellCheck={false}
            />
          </label>

          <label className="text-sm text-slate-200 md:col-span-2" htmlFor="body">
            Body/Query JSON
            <textarea
              id="body"
              value={form.bodyText}
              onChange={(event) => onChange({ bodyText: event.target.value })}
              className="mt-1 h-28 w-full rounded-md border border-slate-700 bg-slate-900/50 px-3 py-2 font-mono text-xs text-slate-100 outline-none focus:border-accent"
              spellCheck={false}
            />
          </label>

          <label className="inline-flex items-center gap-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={form.enabled}
              onChange={(event) => onChange({ enabled: event.target.checked })}
              className="h-4 w-4 rounded border-slate-600 bg-slate-900"
            />
            Enabled
          </label>

          <div className="flex items-center gap-2 md:col-span-2">
            <button
              type="submit"
              disabled={busyKey === 'form'}
              className="rounded-md border border-accent/50 bg-accent/15 px-4 py-2 text-sm text-accent transition hover:border-accent disabled:opacity-60"
            >
              {editingId ? 'Save source' : 'Create source'}
            </button>
            {editingId && (
              <button
                type="button"
                onClick={resetForm}
                className="rounded-md border border-slate-600 px-4 py-2 text-sm text-slate-300 transition hover:border-slate-500"
              >
                Cancel edit
              </button>
            )}
          </div>
        </form>
      </section>

      <section className="card overflow-x-auto">
        <h2 className="text-lg font-semibold">Configured Sources</h2>

        {sortedSources.length === 0 ? (
          <p className="mt-4 text-sm text-muted">No sources configured yet.</p>
        ) : (
          <table className="mt-4 min-w-full border-separate border-spacing-y-2 text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wide text-muted">
                <th className="px-2 py-1">Name</th>
                <th className="px-2 py-1">Service</th>
                <th className="px-2 py-1">Interval</th>
                <th className="px-2 py-1">State</th>
                <th className="px-2 py-1">Last poll</th>
                <th className="px-2 py-1">Ingested</th>
                <th className="px-2 py-1">Actions</th>
              </tr>
            </thead>
            <tbody>
              {sortedSources.map((source) => (
                <tr key={source.id} className="rounded-lg border border-slate-800 bg-slate-900/40">
                  <td className="px-2 py-2 align-top">
                    <p className="font-medium text-slate-100">{source.name}</p>
                    <p className="mt-1 max-w-sm break-all text-xs text-muted">{source.endpoint}</p>
                  </td>
                  <td className="px-2 py-2 align-top text-slate-300">{source.default_service}</td>
                  <td className="px-2 py-2 align-top text-slate-300">{source.poll_interval_seconds}s</td>
                  <td className="px-2 py-2 align-top">
                    <p className={source.enabled ? 'text-normal' : 'text-muted'}>{source.enabled ? 'Enabled' : 'Paused'}</p>
                    <p className={source.last_status === 'error' ? 'text-xs text-critical' : 'text-xs text-muted'}>
                      {source.last_status}
                    </p>
                    {source.last_error && <p className="mt-1 max-w-xs text-xs text-critical">{source.last_error}</p>}
                  </td>
                  <td className="px-2 py-2 align-top text-slate-300">{formatTimestamp(source.last_polled_at)}</td>
                  <td className="px-2 py-2 align-top text-slate-300">{source.total_ingested}</td>
                  <td className="px-2 py-2 align-top">
                    <div className="flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => onEdit(source)}
                        className="rounded border border-slate-600 px-2 py-1 text-xs text-slate-200 hover:border-accent/50"
                      >
                        Edit
                      </button>
                      <button
                        type="button"
                        disabled={busyKey === `sync-${source.id}`}
                        onClick={() => onSync(source)}
                        className="rounded border border-accent/40 px-2 py-1 text-xs text-accent hover:border-accent disabled:opacity-60"
                      >
                        Sync now
                      </button>
                      <button
                        type="button"
                        disabled={busyKey === `toggle-${source.id}`}
                        onClick={() => onToggle(source)}
                        className="rounded border border-warning/50 px-2 py-1 text-xs text-warning hover:border-warning disabled:opacity-60"
                      >
                        {source.enabled ? 'Pause' : 'Enable'}
                      </button>
                      <button
                        type="button"
                        disabled={busyKey === `delete-${source.id}`}
                        onClick={() => onDelete(source)}
                        className="rounded border border-critical/50 px-2 py-1 text-xs text-critical hover:border-critical disabled:opacity-60"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

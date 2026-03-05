export const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? 'http://localhost:8000';

export type Incident = {
  id: string;
  status: 'open' | 'acknowledged' | 'resolved';
  severity: 'critical' | 'warning' | 'info';
  created_at: string;
  updated_at: string;
  service: string;
  summary: string;
  timeline: Array<{ timestamp: string; message: string; severity?: string }>;
  rca?: {
    root_cause: string;
    confidence: number;
    explanation: string;
    suggested_fix: string;
    affected_services: string[];
    evidence: Array<{ timestamp: string; message: string; service: string }>;
  };
};

export type SourceMethod = 'GET' | 'POST';

export type DataSource = {
  id: string;
  name: string;
  endpoint: string;
  default_service: string;
  method: SourceMethod;
  headers: Record<string, string>;
  body: Record<string, unknown> | null;
  response_logs_field: string | null;
  poll_interval_seconds: number;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_polled_at: string | null;
  last_success_at: string | null;
  last_status: 'never' | 'ok' | 'error';
  last_error: string | null;
  total_ingested: number;
  has_auth_token: boolean;
};

export type DataSourceInput = {
  name: string;
  endpoint: string;
  default_service: string;
  method: SourceMethod;
  headers?: Record<string, string>;
  body?: Record<string, unknown> | null;
  response_logs_field?: string | null;
  poll_interval_seconds: number;
  enabled: boolean;
  auth_token?: string | null;
};

export async function fetchIncidents(): Promise<Incident[]> {
  const res = await fetch(`${API_BASE}/incidents?page=1&size=50`, { cache: 'no-store' });
  if (!res.ok) {
    throw new Error('Failed to fetch incidents');
  }
  const data = await res.json();
  return data.items ?? [];
}

export async function fetchIncident(id: string): Promise<Incident> {
  const res = await fetch(`${API_BASE}/incidents/${id}`, { cache: 'no-store' });
  if (!res.ok) {
    throw new Error('Failed to fetch incident detail');
  }
  return res.json();
}

export async function fetchSources(): Promise<DataSource[]> {
  const res = await fetch(`${API_BASE}/sources`, { cache: 'no-store' });
  if (!res.ok) {
    throw new Error('Failed to fetch sources');
  }
  const data = await res.json();
  return data.items ?? [];
}

export async function createSource(input: DataSourceInput): Promise<DataSource> {
  const res = await fetch(`${API_BASE}/sources`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input)
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function updateSource(sourceId: string, input: DataSourceInput): Promise<DataSource> {
  const res = await fetch(`${API_BASE}/sources/${sourceId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input)
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function setSourceEnabled(sourceId: string, enabled: boolean): Promise<DataSource> {
  const action = enabled ? 'enable' : 'disable';
  const res = await fetch(`${API_BASE}/sources/${sourceId}/${action}`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function syncSource(sourceId: string): Promise<{ accepted: number; fetched: number; status: string }> {
  const res = await fetch(`${API_BASE}/sources/${sourceId}/sync`, { method: 'POST' });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function deleteSource(sourceId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/sources/${sourceId}`, { method: 'DELETE' });
  if (!res.ok) {
    throw new Error(await res.text());
  }
}

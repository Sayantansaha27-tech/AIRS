type Props = {
  kind: 'severity' | 'status';
  value: string;
};

const palette: Record<string, string> = {
  critical: 'bg-critical/20 text-critical border-critical/40',
  warning: 'bg-warning/20 text-warning border-warning/40',
  info: 'bg-normal/20 text-normal border-normal/40',
  open: 'bg-critical/20 text-critical border-critical/40',
  acknowledged: 'bg-warning/20 text-warning border-warning/40',
  resolved: 'bg-normal/20 text-normal border-normal/40'
};

export function StatusBadge({ kind, value }: Props) {
  const cls = palette[value] ?? 'bg-slate-700/20 text-slate-300 border-slate-600';
  return (
    <span className={`inline-flex rounded-md border px-2 py-1 text-xs uppercase tracking-wide ${cls}`}>
      {kind}:{value}
    </span>
  );
}

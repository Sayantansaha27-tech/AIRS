'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

const NAV_ITEMS = [
  { href: '/', label: 'Dashboard', match: (path: string) => path === '/' || path.startsWith('/incidents/') },
  { href: '/sources', label: 'Data Sources', match: (path: string) => path.startsWith('/sources') }
];

export function TopNav() {
  const pathname = usePathname();

  return (
    <nav aria-label="Main" className="mb-6 flex flex-wrap items-center gap-2">
      {NAV_ITEMS.map((item) => {
        const active = item.match(pathname);
        return (
          <Link
            key={item.href}
            href={item.href}
            aria-current={active ? 'page' : undefined}
            className={`rounded-md border px-3 py-2 text-sm transition ${
              active
                ? 'border-accent/60 bg-accent/15 text-accent'
                : 'border-slate-700 bg-slate-900/40 text-slate-300 hover:border-accent/50 hover:text-accent'
            }`}
          >
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}

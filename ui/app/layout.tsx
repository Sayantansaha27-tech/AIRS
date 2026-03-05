import type { Metadata } from 'next';
import { IBM_Plex_Sans, JetBrains_Mono } from 'next/font/google';

import './globals.css';

const sans = IBM_Plex_Sans({ subsets: ['latin'], weight: ['400', '500', '600', '700'] });
const mono = JetBrains_Mono({ subsets: ['latin'], variable: '--font-mono' });

export const metadata: Metadata = {
  title: 'AIRS',
  description: 'AI Incident Response System'
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={`${sans.className} ${mono.variable}`}>{children}</body>
    </html>
  );
}

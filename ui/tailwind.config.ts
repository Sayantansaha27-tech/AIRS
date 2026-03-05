import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./app/**/*.{ts,tsx}', './components/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: '#05080b',
        panel: '#11171f',
        muted: '#7f8ea3',
        critical: '#f05b5b',
        warning: '#f8c15c',
        normal: '#4bcc7b',
        accent: '#53d8fb'
      },
      boxShadow: {
        glow: '0 0 0 1px rgba(83,216,251,0.15), 0 20px 40px rgba(2,8,16,0.35)'
      },
      backgroundImage: {
        haze: 'radial-gradient(circle at 20% 15%, rgba(83,216,251,0.15), transparent 40%), radial-gradient(circle at 80% 20%, rgba(240,91,91,0.12), transparent 35%)'
      }
    }
  },
  plugins: []
};

export default config;

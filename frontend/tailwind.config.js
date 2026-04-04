/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        /* ── Fixed semantic tokens ── */
        profit:  '#22c55e',
        loss:    '#f43f5e',
        warning: '#f59e0b',
        info:    '#3b82f6',
        buy:     '#22c55e',
        sell:    '#f43f5e',

        /* ── Theme-reactive via CSS variables (support opacity modifier /NN) ── */
        brand:   'rgb(var(--c-brand) / <alpha-value>)',
        accent:  'rgb(var(--c-accent) / <alpha-value>)',

        'bg-base':     'rgb(var(--c-bg-base)     / <alpha-value>)',
        'bg-surface':  'rgb(var(--c-bg-surface)  / <alpha-value>)',
        'bg-card':     'rgb(var(--c-bg-card)     / <alpha-value>)',
        'bg-elevated': 'rgb(var(--c-bg-elevated) / <alpha-value>)',
        'bg-hover':    'rgb(var(--c-bg-hover)    / <alpha-value>)',
        'bg-input':    'rgb(var(--c-bg-input)    / <alpha-value>)',
        'bg-soft':     'rgb(var(--c-bg-elevated) / <alpha-value>)',

        border:              'rgb(var(--c-border)       / <alpha-value>)',
        'border-dim':        'rgb(var(--c-border)       / <alpha-value>)',
        'border-strong':     'rgb(var(--c-bg-hover)     / <alpha-value>)',

        'text-bright': 'rgb(var(--c-text-bright) / <alpha-value>)',
        'text-pri':    'rgb(var(--c-text-pri)    / <alpha-value>)',
        'text-sec':    'rgb(var(--c-text-sec)    / <alpha-value>)',
        'text-muted':  'rgb(var(--c-text-muted)  / <alpha-value>)',
      },
      fontFamily: {
        sans: ['"Inter"', '"Space Grotesk"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', '"IBM Plex Mono"', 'monospace'],
      },
      borderRadius: {
        DEFAULT: '6px',
        sm: '4px',
        md: '6px',
        lg: '10px',
        xl: '14px',
        '2xl': '18px',
      },
      boxShadow: {
        card:   '0 1px 3px rgba(0,0,0,.5), 0 1px 2px rgba(0,0,0,.3)',
        modal:  '0 24px 64px rgba(0,0,0,.8)',
        brand:  '0 0 20px rgba(34,211,238,.2)',
        profit: '0 0 12px rgba(34,197,94,.2)',
        loss:   '0 0 12px rgba(244,63,94,.2)',
      },
      keyframes: {
        'pulse-glow': {
          '0%,100%': { boxShadow: '0 0 4px rgba(34,211,238,.3)' },
          '50%':     { boxShadow: '0 0 16px rgba(34,211,238,.5)' },
        },
        'flash-green': {
          '0%':   { background: 'rgba(34,197,94,.25)' },
          '100%': { background: 'transparent' },
        },
        'flash-red': {
          '0%':   { background: 'rgba(244,63,94,.25)' },
          '100%': { background: 'transparent' },
        },
        'slide-up': {
          from: { opacity: '0', transform: 'translateY(8px)' },
          to:   { opacity: '1', transform: 'translateY(0)' },
        },
        'slide-down': {
          from: { opacity: '0', transform: 'translateY(-8px)' },
          to:   { opacity: '1', transform: 'translateY(0)' },
        },
        'fade-in': {
          from: { opacity: '0' },
          to:   { opacity: '1' },
        },
        shimmer: {
          '0%':   { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition:  '200% 0' },
        },
        blink: {
          '0%,100%': { opacity: '1' },
          '50%':     { opacity: '0.3' },
        },
      },
      animation: {
        'pulse-glow':  'pulse-glow 2s ease-in-out infinite',
        'flash-green': 'flash-green .8s ease-out',
        'flash-red':   'flash-red .8s ease-out',
        'slide-up':    'slide-up .2s ease-out',
        'slide-down':  'slide-down .2s ease-out',
        'fade-in':     'fade-in .25s ease-out',
        shimmer:       'shimmer 2s linear infinite',
        blink:         'blink 1.4s ease-in-out infinite',
      },
    },
  },
  plugins: [],
}

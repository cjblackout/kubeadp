/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
    "./src/index.html",
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#fff1f0',
          100: '#ffe0de',
          200: '#ffc6c2',
          300: '#ff9a94',
          400: '#f6574f',
          500: '#EA100C',
          600: '#C80D0A',
          700: '#A30B09',
          800: '#850D0B',
          900: '#6B0F0D',
        },
        surface: {
          50: '#f8fafc',
          100: '#f1f5f9',
          200: '#e2e8f0',
          300: '#cbd5e1',
        },
        sidebar: {
          DEFAULT: '#FFFFFF',
          light: '#F8FAFC',
          hover: '#FFF1F0',
          border: '#E2E8F0',
          muted: '#64748B',
          text: '#1E293B',
        },
      },
      fontFamily: {
        display: ['"JetBrains Mono"', 'monospace'],
        body: ['"DM Sans"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'monospace'],
      },
      boxShadow: {
        'ring': '0px 0px 0px 1px rgba(0, 0, 0, 0.06)',
        'card': '0px 0px 0px 1px rgba(0, 0, 0, 0.06), 0px 1px 2px rgba(0, 0, 0, 0.04)',
        'card-hover': '0px 0px 0px 1px rgba(0, 0, 0, 0.06), 0px 2px 4px rgba(0, 0, 0, 0.04), 0px 8px 16px rgba(0, 0, 0, 0.06)',
        'elevated': '0px 0px 0px 1px rgba(0, 0, 0, 0.06), 0px 4px 8px rgba(0, 0, 0, 0.04), 0px 12px 24px rgba(0, 0, 0, 0.06)',
        'modal': '0px 0px 0px 1px rgba(0, 0, 0, 0.06), 0px 8px 16px rgba(0, 0, 0, 0.08), 0px 24px 48px rgba(0, 0, 0, 0.12)',
      },
      animation: {
        'slide-in': 'slide-in 0.3s ease-out',
        'fade-in': 'fade-in 0.2s ease-out',
        'sidebar-in': 'sidebar-in 0.3s cubic-bezier(0.16, 1, 0.3, 1)',
      },
      keyframes: {
        'slide-in': {
          from: { transform: 'translateX(100%)', opacity: '0' },
          to: { transform: 'translateX(0)', opacity: '1' },
        },
        'fade-in': {
          from: { opacity: '0', transform: 'translateY(4px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        'sidebar-in': {
          from: { opacity: '0', transform: 'translateX(-8px)' },
          to: { opacity: '1', transform: 'translateX(0)' },
        },
        'panel-maximize': {
          from: { opacity: '0', transform: 'scale(0.97)' },
          to: { opacity: '1', transform: 'scale(1)' },
        },
      },
    },
  },
  plugins: [],
}

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const BACKEND_TARGET = process.env.VITE_BACKEND_URL ?? 'http://localhost:8418'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('/react') || id.includes('/react-dom') || id.includes('/react-router-dom')) {
            return 'react-vendor'
          }
          if (id.includes('/@radix-ui/')) return 'radix-ui'
          if (id.includes('/lucide-react/')) return 'icons'
          if (id.includes('/recharts/') || id.includes('/d3-')) return 'charts'
          if (
            id.includes('/react-hook-form/') ||
            id.includes('/@hookform/') ||
            id.includes('/zod/') ||
            id.includes('/qrcode/') ||
            id.includes('/date-fns/')
          ) {
            return 'form-utils'
          }
          return
        },
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, '.'),
    },
  },
  server: {
    port: 3010,
    strictPort: true,
    proxy: {
      '/api':     { target: BACKEND_TARGET, changeOrigin: true },
      '/healthz': { target: BACKEND_TARGET, changeOrigin: true },
    },
  },
})

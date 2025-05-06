// --- START OF FILE website/client/vite.config.ts ---
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  appType: 'spa', // <-- ĐẢM BẢO DÒNG NÀY CÓ Ở ĐÂY
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:3001', // Port backend local
        changeOrigin: true,
      }
    }
  }
})
// --- END OF FILE website/client/vite.config.ts ---
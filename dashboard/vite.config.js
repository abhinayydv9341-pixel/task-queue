import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In development the API runs separately on :8000; in production FastAPI serves dist/.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8000' } },
});

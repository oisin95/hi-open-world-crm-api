import { defineConfig } from 'vite';
import { cloudflare } from '@cloudflare/vite-plugin';

// The Cloudflare plugin builds src/worker.ts (declared as `main` in wrangler.json) alongside the static page.
// Webflow Cloud sets `base` to the mount path at build time.
export default defineConfig({
  plugins: [cloudflare()],
});

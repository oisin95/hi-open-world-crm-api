# HI Open World · Brand CRM API

Small backend for the Brand CRM at `openworld.hihostels.com/brand-crm`, deployed as a **Webflow Cloud** app (Cloudflare Workers + D1 SQLite) and mounted at `/crm-api` on the same domain.

- `src/worker.ts` — API: `GET /api/health`, `POST /api/sync` (push field-level changes + pull everything changed since a revision), `GET /api/export` (full JSON backup).
- `src/shared.js` — field-level merge logic shared with the CRM's tests.
- `wrangler.json` — declares the `DB` SQLite binding; Webflow Cloud provisions it on deploy. Tables are created on first request.
- Auth: every `/api/*` call (except health) must carry `X-CRM-Key` (the team password; the Worker compares its SHA-256 to the stored hash) and `X-CRM-User` (one of the team names). Nothing is readable without the password.

Deploy: push to GitHub → Webflow site → Apps → Webflow Cloud → Create app from this repo, mount path `/crm-api`. To rotate the password, change `PASS_HASH` in `src/shared.js` (SHA-256 hex of the new password) and the same constant in the CRM page's head code.

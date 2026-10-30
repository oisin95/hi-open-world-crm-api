/// <reference types="@cloudflare/workers-types" />
// HI Open World · Brand CRM API — runs on Webflow Cloud (Cloudflare Workers + D1).
// Routes: GET /api/health · POST /api/sync · GET /api/export. Everything else serves the static status page.
import { mergeFields, PASS_HASH, ALLOWED_USERS } from './shared.js';

interface Env { ASSETS: Fetcher; DB: D1Database }
type Row = { id: string; data: string; fts: string; rev: number; updated_at: number; updated_by: string };

const MAX_BODY = 4 * 1024 * 1024;
let ready = false;

function cors(req: Request): Record<string, string> {
  const o = req.headers.get('origin') || '';
  const ok = /^https:\/\/([a-z0-9-]+\.)*(hihostels\.com|webflow\.io)$/i.test(o) || o === '';
  return ok && o ? { 'access-control-allow-origin': o, 'access-control-allow-headers': 'content-type, x-crm-key, x-crm-user', 'access-control-allow-methods': 'GET, POST, OPTIONS', 'vary': 'origin' } : {};
}
function json(req: Request, o: unknown, status = 200): Response {
  return new Response(JSON.stringify(o), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...cors(req) } });
}
async function sha256(s: string): Promise<string> {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return Array.from(new Uint8Array(b)).map((x) => x.toString(16).padStart(2, '0')).join('');
}
async function ensure(db: D1Database) {
  if (ready) return;
  await db.batch([
    db.prepare('CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, data TEXT NOT NULL, fts TEXT NOT NULL, rev INTEGER NOT NULL, updated_at INTEGER, updated_by TEXT)'),
    db.prepare('CREATE TABLE IF NOT EXISTS activity (id TEXT PRIMARY KEY, brand_id TEXT, ts INTEGER, rev INTEGER NOT NULL, data TEXT NOT NULL)'),
    db.prepare('CREATE TABLE IF NOT EXISTS added (id TEXT PRIMARY KEY, rev INTEGER NOT NULL, data TEXT NOT NULL)'),
    db.prepare('CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), rev INTEGER NOT NULL)'),
    db.prepare('INSERT OR IGNORE INTO counter (id, rev) VALUES (1, 0)'),
    db.prepare('CREATE INDEX IF NOT EXISTS records_rev ON records(rev)'),
    db.prepare('CREATE INDEX IF NOT EXISTS activity_rev ON activity(rev)'),
    db.prepare('CREATE INDEX IF NOT EXISTS added_rev ON added(rev)'),
  ]);
  ready = true;
}
async function nextRev(db: D1Database): Promise<number> {
  const r = await db.prepare('UPDATE counter SET rev = rev + 1 WHERE id = 1 RETURNING rev').first<{ rev: number }>();
  return r ? r.rev : 0;
}
async function currentRev(db: D1Database): Promise<number> {
  const r = await db.prepare('SELECT rev FROM counter WHERE id = 1').first<{ rev: number }>();
  return r ? r.rev : 0;
}
function rowToRecord(x: Row) {
  return Object.assign(JSON.parse(x.data), { _f: JSON.parse(x.fts), _by: x.updated_by, _at: x.updated_at });
}
async function changes(db: D1Database, since: number) {
  const [r, a, d, c] = await Promise.all([
    db.prepare('SELECT * FROM records WHERE rev > ?').bind(since).all<Row>(),
    db.prepare('SELECT data FROM activity WHERE rev > ? ORDER BY ts DESC LIMIT 5000').bind(since).all<{ data: string }>(),
    db.prepare('SELECT data FROM added WHERE rev > ?').bind(since).all<{ data: string }>(),
    db.prepare('SELECT COUNT(*) AS c FROM records').first<{ c: number }>(),
  ]);
  const records: Record<string, any> = {};
  for (const x of r.results) records[x.id] = rowToRecord(x);
  return { records, activity: a.results.map((x) => JSON.parse(x.data)), added: d.results.map((x) => JSON.parse(x.data)), total: c ? c.c : 0 };
}

async function upsertRecord(db: D1Database, id: string, patch: any, who: string, now: number, out: { conflicts: any[]; applied: number; skipped: number }) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const row = await db.prepare('SELECT * FROM records WHERE id = ?').bind(id).first<Row>();
    const stored = row ? { data: JSON.parse(row.data), fts: JSON.parse(row.fts) } : null;
    const m = mergeFields(stored, patch, now);
    out.skipped += m.conflicts.length;
    for (const c of m.conflicts) out.conflicts.push(Object.assign({ id, keptBy: row ? row.updated_by : '' }, c));
    if (!m.applied.length) return;
    const rev = await nextRev(db);
    const res = row
      ? await db.prepare('UPDATE records SET data = ?, fts = ?, rev = ?, updated_at = ?, updated_by = ? WHERE id = ? AND rev = ?').bind(JSON.stringify(m.data), JSON.stringify(m.fts), rev, now, who, id, row.rev).run()
      : await db.prepare('INSERT OR IGNORE INTO records (id, data, fts, rev, updated_at, updated_by) VALUES (?, ?, ?, ?, ?, ?)').bind(id, JSON.stringify(m.data), JSON.stringify(m.fts), rev, now, who).run();
    if (res.meta.changes > 0) { out.applied += m.applied.length; return; }
    // Another user wrote this record between our read and write: re-read and merge again.
  }
}

async function handleSync(req: Request, env: Env, who: string): Promise<Response> {
  const len = Number(req.headers.get('content-length') || 0);
  if (len > MAX_BODY) return json(req, { error: 'payload too large' }, 413);
  let body: any;
  try { body = await req.json(); } catch { return json(req, { error: 'invalid json' }, 400); }
  const db = env.DB;
  const since = Math.max(0, Number(body.since) || 0);
  const now = Date.now();
  const report = { conflicts: [] as any[], applied: 0, skipped: 0, activityAdded: 0, brandsAdded: 0 };
  const recs = body.records && typeof body.records === 'object' ? body.records : {};
  for (const id of Object.keys(recs).slice(0, 2000)) {
    const p = recs[id];
    if (!id || !p || typeof p !== 'object' || !p.fields || typeof p.fields !== 'object') continue;
    await upsertRecord(db, String(id).slice(0, 200), p, who, now, report);
  }
  const acts = Array.isArray(body.activity) ? body.activity.filter((a: any) => a && a.id && a.brandId).slice(0, 5000) : [];
  if (acts.length) {
    const rev = await nextRev(db);
    const stmt = db.prepare('INSERT OR IGNORE INTO activity (id, brand_id, ts, rev, data) VALUES (?, ?, ?, ?, ?)');
    const res = await db.batch(acts.map((a: any) => stmt.bind(String(a.id), String(a.brandId), Number(a.ts) || now, rev, JSON.stringify(a))));
    report.activityAdded = res.reduce((n, r) => n + (r.meta.changes || 0), 0);
  }
  const added = Array.isArray(body.added) ? body.added.filter((b: any) => b && b.id && b.name).slice(0, 500) : [];
  if (added.length) {
    const rev = await nextRev(db);
    const stmt = db.prepare('INSERT INTO added (id, rev, data) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data, rev = excluded.rev');
    const res = await db.batch(added.map((b: any) => stmt.bind(String(b.id), rev, JSON.stringify(b))));
    report.brandsAdded = res.reduce((n, r) => n + (r.meta.changes || 0), 0);
  }
  const rev = await currentRev(db);
  const ch = await changes(db, since);
  return json(req, { rev, records: ch.records, activity: ch.activity, added: ch.added, seeded: ch.total > 0, total: ch.total, report, user: who, now });
}

async function handleExport(req: Request, env: Env): Promise<Response> {
  const db = env.DB;
  const ch = await changes(db, -1);
  const rev = await currentRev(db);
  return new Response(JSON.stringify({ exportedAt: new Date().toISOString(), rev, records: ch.records, activity: ch.activity, added: ch.added }, null, 1), {
    headers: { 'content-type': 'application/json; charset=utf-8', 'content-disposition': 'attachment; filename="brand-crm-export.json"', 'cache-control': 'no-store', ...cors(req) },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const i = url.pathname.indexOf('/api/');
    if (i < 0) return env.ASSETS ? env.ASSETS.fetch(request) : new Response('Not found', { status: 404 });
    const route = url.pathname.slice(i + 4).replace(/\/+$/, '');
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(request) });
    if (route === '/health') return json(request, { ok: true, service: 'hi-open-world-crm-api', time: new Date().toISOString() });
    if (!env.DB) return json(request, { error: 'database binding missing' }, 500);
    const key = request.headers.get('x-crm-key') || '';
    if (!key || (await sha256(key)) !== PASS_HASH) return json(request, { error: 'unauthorised' }, 401);
    const u = (request.headers.get('x-crm-user') || '').trim();
    const who = ALLOWED_USERS.includes(u) ? u : 'Team';
    try {
      await ensure(env.DB);
      if (route === '/sync' && request.method === 'POST') return handleSync(request, env, who);
      if (route === '/export' && request.method === 'GET') return handleExport(request, env);
      return json(request, { error: 'not found' }, 404);
    } catch (e: any) {
      return json(request, { error: 'server error', detail: String(e && e.message || e) }, 500);
    }
  },
} satisfies ExportedHandler<Env>;

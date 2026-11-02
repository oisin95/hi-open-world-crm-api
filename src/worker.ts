/// <reference types="@cloudflare/workers-types" />
// HI Open World · Brand CRM API — Webflow Cloud (Cloudflare Workers + D1 SQLite). Mounted at /app.
// Routes (all under /api, password-protected except /health):
//   GET  /health            liveness
//   POST /sync              push field-level changes, pull everything changed since a revision
//   GET  /export            full JSON backup
//   GET  /stats             counts, provenance and timestamp distribution (diagnostics)
//   POST /import            controlled migration: {mode:'fill'|'restore', ts, data, dryRun, confirm}
//   DELETE /test            remove disposable test records (ids starting "__test")
import { mergeFields, PASS_HASH, ALLOWED_USERS, SKIP_FIELDS } from './shared.js';

interface Env { ASSETS?: Fetcher; DB: D1Database }
type Row = { id: string; data: string; fts: string; rev: number; updated_at: number; updated_by: string };
type Patch = { fields: Record<string, unknown>; ts: number | Record<string, number> };
type Report = { conflicts: any[]; applied: number; skipped: number; activityAdded: number; brandsAdded: number; retries: number };

const MAX_BODY = 8 * 1024 * 1024;
const BATCH = 60; // statements per D1 batch (each batch is one transaction)
let ready = false;

// Auth is the X-CRM-Key header (never a cookie), so cross-origin requests without it get nothing; open CORS is safe.
const CORS: Record<string, string> = { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type, x-crm-key, x-crm-user', 'access-control-allow-methods': 'GET, POST, DELETE, OPTIONS', 'access-control-max-age': '86400' };
function json(o: unknown, status = 200, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(o), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', ...CORS, ...extra } });
}
async function sha256(s: string): Promise<string> {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return Array.from(new Uint8Array(b)).map((x) => x.toString(16).padStart(2, '0')).join('');
}
function chunk<T>(a: T[], n: number): T[][] { const out: T[][] = []; for (let i = 0; i < a.length; i += n) out.push(a.slice(i, i + n)); return out; }
function strip(rec: any): Record<string, unknown> { const o: Record<string, unknown> = {}; for (const k of Object.keys(rec || {})) if (!(k in SKIP_FIELDS)) o[k] = rec[k]; return o; }

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
const INC = 'UPDATE counter SET rev = rev + 1 WHERE id = 1';
const REV = '(SELECT rev FROM counter WHERE id = 1)';

async function readRows(db: D1Database, ids: string[]): Promise<Record<string, Row>> {
  const out: Record<string, Row> = {};
  for (const part of chunk(ids, 90)) {
    const r = await db.prepare('SELECT * FROM records WHERE id IN (' + part.map(() => '?').join(',') + ')').bind(...part).all<Row>();
    for (const row of r.results) out[row.id] = row;
  }
  return out;
}
function rowToRecord(x: Row) { return Object.assign(JSON.parse(x.data), { _f: JSON.parse(x.fts), _by: x.updated_by, _at: x.updated_at }); }

/** Field-level merge + write for many records. Each batch = one transaction containing the counter increment,
 *  so a reader can never observe the new revision without the rows that carry it (no gaps). Optimistic `rev`
 *  guard per row; rows changed concurrently by someone else are re-read and re-merged (up to 4 passes). */
async function writeRecords(db: D1Database, items: { id: string; patch: Patch }[], who: string, now: number, report: Report, dryRun = false) {
  let pending = items;
  const seen = new Set<string>();
  for (let attempt = 0; attempt < 4 && pending.length; attempt++) {
    const rows = await readRows(db, pending.map((x) => x.id));
    const plans: { id: string; stmt: D1PreparedStatement; n: number }[] = [];
    for (const it of pending) {
      const row = rows[it.id];
      const m = mergeFields(row ? { data: JSON.parse(row.data), fts: JSON.parse(row.fts) } : null, it.patch, now);
      for (const c of m.conflicts) { const k = it.id + '\u0000' + c.field; if (!seen.has(k)) { seen.add(k); report.skipped++; report.conflicts.push(Object.assign({ id: it.id, keptBy: row ? row.updated_by : '' }, c)); } }
      if (!m.applied.length) continue;
      if (dryRun) { report.applied += m.applied.length; continue; }
      const stmt = row
        ? db.prepare('UPDATE records SET data = ?, fts = ?, rev = ' + REV + ', updated_at = ?, updated_by = ? WHERE id = ? AND rev = ?').bind(JSON.stringify(m.data), JSON.stringify(m.fts), now, who, it.id, row.rev)
        : db.prepare('INSERT OR IGNORE INTO records (id, data, fts, rev, updated_at, updated_by) VALUES (?, ?, ?, ' + REV + ', ?, ?)').bind(it.id, JSON.stringify(m.data), JSON.stringify(m.fts), now, who);
      plans.push({ id: it.id, stmt, n: m.applied.length });
    }
    if (dryRun || !plans.length) return;
    const retry: { id: string; patch: Patch }[] = [];
    for (const part of chunk(plans, BATCH)) {
      const res = await db.batch([db.prepare(INC)].concat(part.map((p) => p.stmt)));
      part.forEach((p, i) => { const r = res[i + 1]; if (r && r.meta && r.meta.changes > 0) report.applied += p.n; else retry.push(pending.find((x) => x.id === p.id)!); });
    }
    if (retry.length) report.retries++;
    pending = retry;
  }
}
async function writeActivity(db: D1Database, acts: any[], now: number, report: Report) {
  for (const part of chunk(acts, BATCH)) {
    const res = await db.batch([db.prepare(INC)].concat(part.map((a) => db.prepare('INSERT OR IGNORE INTO activity (id, brand_id, ts, rev, data) VALUES (?, ?, ?, ' + REV + ', ?)').bind(String(a.id), String(a.brandId), Number(a.ts) || now, JSON.stringify(a)))));
    report.activityAdded += res.slice(1).reduce((n, r) => n + ((r.meta && r.meta.changes) || 0), 0);
  }
}
async function writeAdded(db: D1Database, items: any[], report: Report) {
  for (const part of chunk(items, BATCH)) {
    const res = await db.batch([db.prepare(INC)].concat(part.map((b) => db.prepare('INSERT INTO added (id, rev, data) VALUES (?, ' + REV + ', ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data, rev = excluded.rev WHERE added.data <> excluded.data').bind(String(b.id), JSON.stringify(b)))));
    report.brandsAdded += res.slice(1).reduce((n, r) => n + ((r.meta && r.meta.changes) || 0), 0);
  }
}
/** One transaction: counter + everything with rev > since, so the returned rev is exactly consistent with the rows. */
async function snapshot(db: D1Database, since: number) {
  const [c, r, a, d, n] = await db.batch([
    db.prepare('SELECT rev FROM counter WHERE id = 1'),
    db.prepare('SELECT * FROM records WHERE rev > ?').bind(since),
    db.prepare('SELECT data FROM activity WHERE rev > ? ORDER BY ts DESC LIMIT 8000').bind(since),
    db.prepare('SELECT data FROM added WHERE rev > ?').bind(since),
    db.prepare('SELECT COUNT(*) AS c FROM records'),
  ]);
  const records: Record<string, any> = {};
  for (const x of (r.results as Row[])) records[x.id] = rowToRecord(x);
  return { rev: Number((c.results[0] as any)?.rev || 0), records, activity: (a.results as any[]).map((x) => JSON.parse(x.data)), added: (d.results as any[]).map((x) => JSON.parse(x.data)), total: Number((n.results[0] as any)?.c || 0) };
}
function newReport(): Report { return { conflicts: [], applied: 0, skipped: 0, activityAdded: 0, brandsAdded: 0, retries: 0 }; }
function cleanPatches(recs: any): { id: string; patch: Patch }[] {
  const out: { id: string; patch: Patch }[] = [];
  if (!recs || typeof recs !== 'object') return out;
  for (const id of Object.keys(recs).slice(0, 3000)) {
    const p = recs[id];
    if (!id || !p || typeof p !== 'object' || !p.fields || typeof p.fields !== 'object') continue;
    out.push({ id: String(id).slice(0, 200), patch: { fields: p.fields, ts: p.ts } });
  }
  return out;
}

async function handleSync(req: Request, env: Env, who: string): Promise<Response> {
  let body: any;
  try { body = await req.json(); } catch { return json({ error: 'invalid json' }, 400); }
  const db = env.DB, since = Math.max(0, Number(body.since) || 0), now = Date.now(), report = newReport();
  await writeRecords(db, cleanPatches(body.records), who, now, report);
  const acts = Array.isArray(body.activity) ? body.activity.filter((a: any) => a && a.id && a.brandId && !a.sample).slice(0, 8000) : [];
  if (acts.length) await writeActivity(db, acts, now, report);
  const added = Array.isArray(body.added) ? body.added.filter((b: any) => b && b.id && b.name).slice(0, 500) : [];
  if (added.length) await writeAdded(db, added, report);
  const s = await snapshot(db, since);
  return json({ rev: s.rev, records: s.records, activity: s.activity, added: s.added, seeded: s.total > 0, total: s.total, report, user: who, now });
}

async function handleExport(env: Env): Promise<Response> {
  const s = await snapshot(env.DB, -1);
  return new Response(JSON.stringify({ format: 'hiow-crm-db-export', exportedAt: new Date().toISOString(), rev: s.rev, records: s.records, activity: s.activity, added: s.added }, null, 1), {
    headers: { 'content-type': 'application/json; charset=utf-8', 'content-disposition': 'attachment; filename="brand-crm-shared-export.json"', 'cache-control': 'no-store', ...CORS },
  });
}

async function handleStats(env: Env): Promise<Response> {
  const s = await snapshot(env.DB, -1);
  const byUser: Record<string, number> = {}, tsBands = { seeded_ts1: 0, imported_ts2: 0, live: 0 }, fieldsByUser: Record<string, number> = {};
  let fields = 0, minAt = 0, maxAt = 0;
  for (const id of Object.keys(s.records)) {
    const r = s.records[id];
    byUser[r._by || '?'] = (byUser[r._by || '?'] || 0) + 1;
    if (r._at) { minAt = minAt ? Math.min(minAt, r._at) : r._at; maxAt = Math.max(maxAt, r._at); }
    for (const f of Object.keys(r._f || {})) { fields++; const t = r._f[f]; if (t <= 1) tsBands.seeded_ts1++; else if (t <= 2) tsBands.imported_ts2++; else tsBands.live++; }
  }
  const actBy: Record<string, number> = {};
  for (const a of s.activity) actBy[a.who || '?'] = (actBy[a.who || '?'] || 0) + 1;
  return json({ rev: s.rev, records: Object.keys(s.records).length, fields, fieldTimestampBands: tsBands, recordsLastUpdatedBy: byUser, activity: s.activity.length, activityBy: actBy, added: s.added.map((b: any) => b.name), firstUpdate: minAt ? new Date(minAt).toISOString() : null, lastUpdate: maxAt ? new Date(maxAt).toISOString() : null, latestActivity: s.activity.slice(0, 8).map((a: any) => ({ who: a.who, type: a.type, at: a.ts ? new Date(a.ts).toISOString() : null, text: String(a.text || '').slice(0, 90) })), fieldsByUser });
}

/** Controlled migration. mode 'fill': every field is offered with timestamp `ts` (default 2) — newer than the
 *  uncontrolled first-sync seed (1) but older than any live edit, so it fills gaps and replaces seed values but can
 *  never overwrite a change made in the CRM. Idempotent: re-running the same file changes nothing.
 *  mode 'restore' (confirm:'RESTORE'): wipe and reload exactly, for rollback from an export. */
async function handleImport(req: Request, env: Env, who: string): Promise<Response> {
  let body: any;
  try { body = await req.json(); } catch { return json({ error: 'invalid json' }, 400); }
  const src = body.data && body.data.data && body.data.data.records ? body.data.data : body.data && body.data.records ? body.data : null;
  if (!src) return json({ error: 'no records in payload' }, 400);
  const db = env.DB, mode = body.mode === 'restore' ? 'restore' : 'fill', dryRun = !!body.dryRun, now = Date.now(), report = newReport();
  const ts = Number(body.ts) || 2;
  const recs = src.records || {}, ids = Object.keys(recs);
  if (mode === 'restore') {
    if (body.confirm !== 'RESTORE') return json({ error: 'restore requires confirm:"RESTORE"' }, 400);
    if (!dryRun) {
      await db.batch([db.prepare('DELETE FROM records'), db.prepare('DELETE FROM activity'), db.prepare('DELETE FROM added'), db.prepare(INC)]);
      const items = ids.map((id) => ({ id, patch: { fields: strip(recs[id]), ts: recs[id]._f && typeof recs[id]._f === 'object' ? recs[id]._f : ts } as Patch }));
      await writeRecords(db, items, who + ' (restore)', now, report);
    } else report.applied = ids.length;
  } else {
    const items = ids.map((id) => ({ id, patch: { fields: strip(recs[id]), ts } as Patch }));
    await writeRecords(db, items, who + ' (import)', now, report, dryRun);
  }
  const acts = Array.isArray(src.activity) ? src.activity.filter((a: any) => a && a.id && a.brandId && !a.sample) : [];
  const added = Array.isArray(src.added) ? src.added.filter((b: any) => b && b.id && b.name) : [];
  if (dryRun) {
    let have = 0;
    for (const part of chunk(acts.map((a: any) => String(a.id)), 90)) { const r = await db.prepare('SELECT COUNT(*) AS c FROM activity WHERE id IN (' + part.map(() => '?').join(',') + ')').bind(...part).first<{ c: number }>(); have += r ? r.c : 0; }
    report.activityAdded = acts.length - have;
    let haveAdd = 0;
    for (const part of chunk(added.map((b: any) => String(b.id)), 90)) { const r = await db.prepare('SELECT COUNT(*) AS c FROM added WHERE id IN (' + part.map(() => '?').join(',') + ')').bind(...part).first<{ c: number }>(); haveAdd += r ? r.c : 0; }
    report.brandsAdded = added.length - haveAdd;
  } else {
    if (acts.length) await writeActivity(db, acts, now, report);
    if (added.length) await writeAdded(db, added, report);
  }
  const s = await snapshot(db, -1);
  return json({ ok: true, mode, dryRun, ts, source: { records: ids.length, activity: acts.length, added: added.length }, report, after: { rev: s.rev, records: s.total, activity: s.activity.length, added: s.added.length } });
}

async function handleTestPurge(env: Env): Promise<Response> {
  const res = await env.DB.batch([db_(env).prepare("DELETE FROM records WHERE substr(id, 1, 6) = '__test'"), db_(env).prepare("DELETE FROM activity WHERE substr(brand_id, 1, 6) = '__test'"), db_(env).prepare("DELETE FROM added WHERE substr(id, 1, 6) = '__test'"), db_(env).prepare(INC)]);
  return json({ ok: true, removed: res.slice(0, 3).map((r) => (r.meta && r.meta.changes) || 0) });
}
function db_(env: Env) { return env.DB; }

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const i = url.pathname.indexOf('/api/');
    if (i < 0) return env.ASSETS ? env.ASSETS.fetch(request) : new Response('Not found', { status: 404 });
    const route = url.pathname.slice(i + 4).replace(/\/+$/, '');
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (route === '/health') return json({ ok: true, service: 'hi-open-world-crm-api', version: '2.0.0', time: new Date().toISOString() });
    if (!env.DB) return json({ error: 'database binding missing' }, 500);
    if (Number(request.headers.get('content-length') || 0) > MAX_BODY) return json({ error: 'payload too large' }, 413);
    const key = request.headers.get('x-crm-key') || '';
    if (!key || (await sha256(key)) !== PASS_HASH) return json({ error: 'unauthorised' }, 401);
    const u = (request.headers.get('x-crm-user') || '').trim();
    const who = ALLOWED_USERS.includes(u) ? u : 'Team';
    try {
      await ensure(env.DB);
      if (route === '/sync' && request.method === 'POST') return handleSync(request, env, who);
      if (route === '/export' && request.method === 'GET') return handleExport(env);
      if (route === '/stats' && request.method === 'GET') return handleStats(env);
      if (route === '/import' && request.method === 'POST') return handleImport(request, env, who);
      if (route === '/test' && request.method === 'DELETE') return handleTestPurge(env);
      return json({ error: 'not found' }, 404);
    } catch (e: any) {
      return json({ error: 'server error', detail: String((e && e.message) || e) }, 500);
    }
  },
} satisfies ExportedHandler<Env>;

// Shared between the Worker (server) and the test harness. Plain ESM, no dependencies.

// SHA-256 of the team password used by the CRM gate. The password itself is never stored here.
export const PASS_HASH = '7a9724e5bef539c497bc0ce3ecb3ac831efbe0e8b9fe2b5ee40d26c596bac07e';
export const ALLOWED_USERS = ['Oisin', 'Cecile', 'Kaori', 'Niall'];
export const SKIP_FIELDS = { _f: 1, _ts: 1, _by: 1, _at: 1 };

/**
 * Field-level last-writer-wins merge for one brand record.
 * stored: { data: {field: value}, fts: {field: ts} } | null
 * patch:  { fields: {field: value}, ts: number | {field: ts} }
 * Returns { data, fts, applied: [field], conflicts: [{field, yours, kept, keptAt}] }
 * A field is applied only when its timestamp is strictly newer than the stored one,
 * so two users editing different fields of the same record both keep their change,
 * and an older edit never silently overwrites a newer one.
 */
export function mergeFields(stored, patch, now) {
  const data = Object.assign({}, (stored && stored.data) || {});
  const fts = Object.assign({}, (stored && stored.fts) || {});
  const applied = [], conflicts = [];
  const fields = (patch && patch.fields) || {};
  for (const f of Object.keys(fields)) {
    if (SKIP_FIELDS[f]) continue;
    const t = typeof patch.ts === 'object' && patch.ts ? Number(patch.ts[f] || now) : Number(patch.ts || now);
    const cur = Number(fts[f] || 0);
    if (t > cur) { data[f] = fields[f]; fts[f] = t; applied.push(f); }
    else if (JSON.stringify(data[f]) !== JSON.stringify(fields[f])) conflicts.push({ field: f, yours: fields[f], kept: data[f], keptAt: cur });
  }
  return { data, fts, applied, conflicts };
}

/** Client-side: list the fields of `cur` that differ from `prev` (both plain objects). */
export function changedFields(cur, prev) {
  const out = {};
  for (const f of Object.keys(cur || {})) {
    if (SKIP_FIELDS[f]) continue;
    if (!prev || JSON.stringify(cur[f]) !== JSON.stringify(prev[f])) out[f] = cur[f];
  }
  return out;
}

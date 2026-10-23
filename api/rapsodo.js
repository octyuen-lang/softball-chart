// Vercel serverless function: stores Rapsodo pitching sessions in the same Upstash Redis database.
// Uses the same environment variables as api/games.js (TEAM_PASSCODE + the Upstash Redis variables).
// One record = one player on one day. Re-uploading the same day replaces it (the page merges by Pitch ID first).
const crypto = require('crypto');

const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOK = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const PASS = process.env.TEAM_PASSCODE;
const META = 'sb:rmeta';
const ALIAS = 'sb:alias'; // shared name / pitch-type mappings used to link Rapsodo to charted games
const key = id => 'sb:r:' + id;
const LINKS = 'sb:links'; // charted pitcher name -> Rapsodo player key (manual overrides)

async function redis(cmds) {
  const r = await fetch(URL_.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOK, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  const j = await r.json();
  if (!Array.isArray(j)) throw new Error(j && j.error ? j.error : 'Storage error');
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}

function authed(req) {
  const a = Buffer.from(String(req.headers['x-passcode'] || '')), b = Buffer.from(PASS);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const clip = (v, n) => String(v == null ? '' : v).slice(0, n);
const validId = id => typeof id === 'string' && /^[A-Za-z0-9_-]{1,80}$/.test(id);

function pairs(flat) {
  const out = [];
  for (let i = 0; i < (flat || []).length; i += 2) { try { out.push(JSON.parse(flat[i + 1])); } catch (e) {} }
  return out;
}

const byDate = (a, b) => (String(b.date) > String(a.date) ? 1 : String(b.date) < String(a.date) ? -1 : (b.id > a.id ? 1 : -1));

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!PASS) return res.status(500).json({ error: 'TEAM_PASSCODE is not set. Add it under Vercel > Settings > Environment Variables, then redeploy.' });
  if (!URL_ || !TOK) return res.status(500).json({ error: 'The database is not connected to this project. Add the Upstash Redis integration and redeploy.' });
  if (!authed(req)) return res.status(401).json({ error: 'Wrong passcode.' });

  try {
    const q = req.query || {};

    if (req.method === 'GET') {
      if (q.links) {
        const [v] = await redis([['GET', LINKS]]);
        let links = {}; try { links = v ? JSON.parse(v) : {}; } catch (e) {}
        return res.status(200).json({ links });
      }
      if (q.aliases) {
        const [v] = await redis([['GET', ALIAS]]);
        let a = {}; try { a = v ? JSON.parse(v) : {}; } catch (e) {}
        return res.status(200).json({ aliases: { players: a.players || {}, types: a.types || {} } });
      }
      if (q.id) {
        if (!validId(q.id)) return res.status(400).json({ error: 'Bad session id.' });
        const [v] = await redis([['GET', key(q.id)]]);
        return v ? res.status(200).json(JSON.parse(v)) : res.status(404).json({ error: 'Session not found.' });
      }
      const [h] = await redis([['HGETALL', META]]);
      const metas = pairs(h).sort(byDate);
      if (q.ids) { // several full sessions at once: ?ids=a,b,c (max 20)
        const ids = String(q.ids).split(',').filter(validId).slice(0, 20);
        if (!ids.length) return res.status(200).json({ sessions: [] });
        const [vals] = await redis([['MGET', ...ids.map(key)]]);
        return res.status(200).json({ sessions: vals.filter(Boolean).map(v => JSON.parse(v)) });
      }
      return res.status(200).json({ sessions: metas });
    }

    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = null; } }

    if (req.method === 'POST' && body && body.links) {
      const L = {}, ent = Object.entries(body.links).slice(0, 300);
      ent.forEach(([k, v]) => { L[clip(k, 80)] = clip(v, 80); });
      await redis([['SET', LINKS, JSON.stringify(L)]]);
      return res.status(200).json({ ok: true, links: L });
    }

    if (req.method === 'POST' && body && body.aliases) {
      const clean = o => { const out = {}; Object.keys(o || {}).slice(0, 300).forEach(k => { out[clip(k, 80)] = clip(o[k], 80); }); return out; };
      const a = { players: clean(body.aliases.players), types: clean(body.aliases.types) };
      await redis([['SET', ALIAS, JSON.stringify(a)]]);
      return res.status(200).json({ ok: true, aliases: a });
    }

    if (req.method === 'POST') {
      const m = body && body.meta, header = body && body.header, rows = body && body.rows;
      if (!m || !validId(m.id)) return res.status(400).json({ error: 'Missing or invalid session id.' });
      if (!Array.isArray(header) || header.length > 60 || !Array.isArray(rows) || rows.length > 3000 || !rows.every(Array.isArray)) {
        return res.status(400).json({ error: 'Missing pitch data.' });
      }
      const meta = {
        id: m.id, player: clip(m.player, 80), playerId: clip(m.playerId, 40), date: clip(m.date, 10),
        names: Array.isArray(m.names) ? m.names.slice(0, 40).map(x => clip(x, 60)) : [],
        pitches: rows.length, updated: Date.now()
      };
      await redis([['SET', key(m.id), JSON.stringify({ meta, header, rows })], ['HSET', META, m.id, JSON.stringify(meta)]]);
      return res.status(200).json({ ok: true, meta });
    }

    if (req.method === 'DELETE') {
      if (!validId(q.id)) return res.status(400).json({ error: 'Bad session id.' });
      await redis([['DEL', key(q.id)], ['HDEL', META, q.id]]);
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed.' });
  } catch (e) {
    return res.status(500).json({ error: 'Server error: ' + (e && e.message ? e.message : e) });
  }
};

// Vercel serverless function: stores every charted game in an Upstash Redis database.
// Needs these environment variables (Vercel > Project > Settings > Environment Variables):
//   TEAM_PASSCODE                      a shared passcode you choose
//   KV_REST_API_URL + KV_REST_API_TOKEN (added automatically by the Upstash Redis integration)
//   (UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN also work)
const crypto = require('crypto');

const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOK = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const PASS = process.env.TEAM_PASSCODE;
const META = 'sb:meta';
const key = id => 'sb:g:' + id;

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
const validId = id => typeof id === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(id);

function pairs(flat) { // HGETALL returns [field, value, field, value...]
  const out = [];
  for (let i = 0; i < (flat || []).length; i += 2) { try { out.push(JSON.parse(flat[i + 1])); } catch (e) {} }
  return out;
}

const byDate = (a, b) => (String(b.date) > String(a.date) ? 1 : String(b.date) < String(a.date) ? -1 : (b.id > a.id ? 1 : -1));

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!PASS) return res.status(500).json({ error: 'TEAM_PASSCODE is not set. Add it under Vercel > Settings > Environment Variables, then redeploy.' });
  if (!URL_ || !TOK) {
    const seen = Object.keys(process.env).filter(k => /^(KV|UPSTASH|REDIS)/i.test(k));
    return res.status(500).json({ error: 'The database is not connected to this project. Add the Upstash Redis integration (Vercel > Storage / Marketplace) and redeploy. Variables found: ' + (seen.join(', ') || 'none') });
  }
  if (!authed(req)) return res.status(401).json({ error: 'Wrong passcode.' });

  try {
    const q = req.query || {};

    if (req.method === 'GET') {
      if (q.id) {
        if (!validId(q.id)) return res.status(400).json({ error: 'Bad game id.' });
        const [v] = await redis([['GET', key(q.id)]]);
        return v ? res.status(200).json(JSON.parse(v)) : res.status(404).json({ error: 'Game not found.' });
      }
      const [h] = await redis([['HGETALL', META]]);
      const metas = pairs(h).sort(byDate);
      if (q.all) {
        const off = Math.max(0, parseInt(q.offset, 10) || 0), lim = Math.min(parseInt(q.limit, 10) || 20, 30);
        const slice = metas.slice(off, off + lim);
        if (!slice.length) return res.status(200).json({ games: [] });
        const [vals] = await redis([['MGET', ...slice.map(m => key(m.id))]]);
        return res.status(200).json({ games: vals.filter(Boolean).map(v => JSON.parse(v)) });
      }
      return res.status(200).json({ games: metas });
    }

    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = null; } }

    if (req.method === 'POST') {
      const g = body && body.game, header = body && body.header, rows = body && body.rows;
      if (!g || !validId(g.id)) return res.status(400).json({ error: 'Missing or invalid game id.' });
      if (!Array.isArray(header) || header.length > 80 || !Array.isArray(rows) || rows.length > 6000 || !rows.every(Array.isArray)) {
        return res.status(400).json({ error: 'Missing pitch data.' });
      }
      const meta = {
        id: g.id, date: clip(g.date, 32), home: clip(g.home, 80), away: clip(g.away, 80), venue: clip(g.venue, 120),
        homeScore: clip(g.homeScore, 6), awayScore: clip(g.awayScore, 6), innings: clip(g.innings, 3),
        type: g.type === 'scrimmage' ? 'scrimmage' : 'game', pitches: rows.length, updated: Date.now()
      };
      await redis([['SET', key(g.id), JSON.stringify({ meta, header, rows })], ['HSET', META, g.id, JSON.stringify(meta)]]);
      return res.status(200).json({ ok: true, meta });
    }

    if (req.method === 'PATCH') {
      if (!body || !validId(body.id)) return res.status(400).json({ error: 'Bad game id.' });
      const [v] = await redis([['GET', key(body.id)]]);
      if (!v) return res.status(404).json({ error: 'Game not found.' });
      const full = JSON.parse(v);
      if (body.type === 'game' || body.type === 'scrimmage') full.meta.type = body.type;
      if (body.date) full.meta.date = clip(body.date, 32);
      await redis([['SET', key(body.id), JSON.stringify(full)], ['HSET', META, body.id, JSON.stringify(full.meta)]]);
      return res.status(200).json({ ok: true, meta: full.meta });
    }

    if (req.method === 'DELETE') {
      if (!validId(q.id)) return res.status(400).json({ error: 'Bad game id.' });
      await redis([['DEL', key(q.id)], ['HDEL', META, q.id]]);
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed.' });
  } catch (e) {
    return res.status(500).json({ error: 'Server error: ' + (e && e.message ? e.message : e) });
  }
};

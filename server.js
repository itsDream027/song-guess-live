const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

process.on('uncaughtException', (e) => console.error('uncaught', e));
process.on('unhandledRejection', (e) => console.error('unhandled', e));

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/admin', (_, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/healthz', (_, res) => res.send('ok'));
app.get('/api/cover/:id', async (req, res) => {
  const id = String(req.params.id || '');
  if (!/^[\w-]{11}$/.test(id)) return res.status(400).end();
  try {
    const r = await fetch('https://i.ytimg.com/vi/' + id + '/hqdefault.jpg', { signal: AbortSignal.timeout(5000) });
    if (!r.ok) return res.status(404).end();
    res.set({ 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=86400' }).send(Buffer.from(await r.arrayBuffer()));
  } catch (_) { res.status(502).end(); }
});
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
const send = (ws, o) => { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (_) {} } };

// ---------- คลังเพลง (พิมพ์เอง) ----------
const FILE = path.join(process.env.DATA_DIR || __dirname, 'songs.json');
let lib = { rev: 0, songs: [] };
try { const j = JSON.parse(fs.readFileSync(FILE, 'utf8')); if (j && Array.isArray(j.songs)) lib = j; } catch (_) {}
const persist = () => { const t = FILE + '.tmp'; fs.writeFile(t, JSON.stringify(lib), (e) => { if (!e) fs.rename(t, FILE, () => {}); }); };
const VID = /^[\w-]{11}$/;
const AMID = /^\d{4,20}$/;
function okArt(a) { try { const u = new URL(String(a || '')); return u.protocol === 'https:' && /\.mzstatic\.com$/.test(u.hostname) ? u.href.slice(0, 400) : ''; } catch (_) { return ''; } }
function cleanSongs(arr) {
  if (!Array.isArray(arr)) return null;
  return arr.slice(0, 500).map((s) => {
    if (!s || typeof s !== 'object') return null;
    const yt = String(s.yt || ''), am = String(s.am || ''), title = String(s.title || '').trim().slice(0, 120);
    if (!(VID.test(yt) || AMID.test(am)) || !title) return null;
    const bpm = Number(s.bpm), b0 = Number(s.beat0);
    const st = s.start === null || s.start === undefined || s.start === '' ? null : Number(s.start);
    return {
      id: String(s.id || yt || am).slice(0, 24), title, yt: VID.test(yt) ? yt : '', am: AMID.test(am) ? am : '', art: okArt(s.art),
      artist: String(s.artist || '').trim().slice(0, 80),
      start: Number.isFinite(st) ? Math.max(0, Math.min(7200, st)) : null,
      hint: String(s.hint || '').slice(0, 600),
      info: String(s.info || '').slice(0, 200),
      auto: s.auto !== false,
      bpm: Number.isFinite(bpm) && bpm >= 40 && bpm <= 240 ? bpm : null,
      beat0: Number.isFinite(b0) ? Math.max(0, Math.min(600, b0)) : 0,
      alts: (Array.isArray(s.alts) ? s.alts : []).map((a) => String(a).trim().slice(0, 120)).filter(Boolean).slice(0, 6),
    };
  }).filter(Boolean);
}

const meta = new Map(); // videoId -> { title, author } (แคช)
async function loadMeta(ids) {
  if (meta.size > 3000) meta.clear();
  const todo = ids.filter((id) => !meta.has(id));
  for (let i = 0; i < todo.length; i += 6) {
    await Promise.all(todo.slice(i, i + 6).map(async (id) => {
      try {
        const r = await fetch('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent('https://www.youtube.com/watch?v=' + id), { signal: AbortSignal.timeout(6000) });
        const j = await r.json();
        meta.set(id, { title: String(j.title || ''), author: String(j.author_name || '') });
      } catch (_) {}
    }));
  }
}

// ---------- เนื้อเพลงอัตโนมัติ (ส่งเฉพาะช่วงที่ใช้ในรอบ ไม่ส่งทั้งเพลง) ----------
const lyCache = new Map();
const nrm = (x) => String(x).toLowerCase().replace(/[\s\-_.,!?'"()]/g, '');
const cleanT = (t) => String(t || '').replace(/[\(\[【].*?[\)\]】]/g, '').replace(/\s*(feat\.?|ft\.?|featuring)\s.*$/i, '').trim();
function parseLRC(t) {
  return t.split('\n').map((l) => { const m = l.match(/^\[(\d+):(\d+(?:\.\d+)?)\](.*)/); return m ? { t: +m[1] * 60 + +m[2], s: m[3].trim() } : null; }).filter((x) => x && x.s.length > 2);
}
function chorusStart(L) { // ท่อนฮุก = กลุ่มบรรทัดที่ซ้ำกลุ่มแรก ถ้าไม่มีให้ใช้ช่วงราว 35% ของเพลง
  const cnt = {}; L.forEach((x) => { const k = nrm(x.s); cnt[k] = (cnt[k] || 0) + 1; });
  const rep = L.map((x) => cnt[nrm(x.s)] >= 2);
  let i = rep.findIndex((v, k) => v && rep[k + 1]); if (i < 0) i = rep.indexOf(true);
  return i >= 0 ? L[i].t : L[Math.floor(L.length * 0.35)].t;
}
async function findLyrics(title, artist) {
  const url = 'https://lrclib.net/api/search?track_name=' + encodeURIComponent(title) + (artist ? '&artist_name=' + encodeURIComponent(artist) : '');
  const j = await (await fetch(url, { signal: AbortSignal.timeout(5000) })).json();
  const t = nrm(cleanT(title)), a = nrm(artist || '');
  const ok = (x) => {
    const n = nrm(cleanT(x.trackName || ''));
    if (n.length < 2 || !(n === t || (n.length >= 3 && (t.includes(n) || n.includes(t))))) return false;
    if (!a) return true;
    const xa = nrm(x.artistName || '');
    return xa.length >= 2 && (xa.includes(a) || a.includes(xa));
  };
  const c = (Array.isArray(j) ? j : []).filter(ok);
  return c.find((x) => x.syncedLyrics && nrm(cleanT(x.trackName)) === t) || c.find((x) => x.syncedLyrics) || c[0] || null;
}
app.get('/api/lyrics', async (req, res) => {
  const title = String(req.query.title || '').slice(0, 120), artist = String(req.query.artist || '').slice(0, 80);
  const from = req.query.from === undefined || req.query.from === '' ? null : Number(req.query.from);
  if (!title) return res.json({});
  const key = nrm(title) + '|' + nrm(artist);
  let hit = lyCache.get(key);
  if (hit === undefined) {
    hit = null;
    try {
      const x = await findLyrics(title, artist);
      if (x) hit = { dur: x.duration || 0, synced: x.syncedLyrics ? parseLRC(x.syncedLyrics) : null, plain: x.plainLyrics ? x.plainLyrics.split('\n').map((l) => l.trim()).filter((l) => l.length > 5) : [] };
    } catch (_) { return res.json({}); }
    if (lyCache.size > 300) lyCache.clear();
    lyCache.set(key, hit);
  }
  if (!hit) return res.json({});
  const out = { dur: hit.dur };
  if (hit.synced && hit.synced.length) {
    out.hook = chorusStart(hit.synced);
    const f = from !== null && Number.isFinite(from) ? from : out.hook;
    out.synced = hit.synced.filter((l) => l.t >= f - 6 && l.t <= f + 45);
  } else if (hit.plain.length) {
    const s0 = Math.floor(hit.plain.length * 0.3); out.plain = hit.plain.slice(s0, s0 + 6);
  }
  res.json(out);
});

// ---------- Apple Music (ไม่บังคับ: ต้องตั้งค่าตัวแปร APPLE_TEAM_ID / APPLE_KEY_ID / APPLE_PRIVATE_KEY) ----------
const AP = { team: process.env.APPLE_TEAM_ID || '', key: process.env.APPLE_KEY_ID || '', pem: (process.env.APPLE_PRIVATE_KEY || '').replace(/\\n/g, '\n') };
const appleOn = () => !!(AP.team && AP.key && AP.pem);
let apTok = null;
function appleToken() {
  const now = Math.floor(Date.now() / 1000);
  if (apTok && apTok.exp - 600 > now) return apTok.t;
  const b = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const head = b({ alg: 'ES256', kid: AP.key }), exp = now + 12 * 3600, body = b({ iss: AP.team, iat: now, exp });
  const sig = crypto.sign('sha256', Buffer.from(head + '.' + body), { key: AP.pem, dsaEncoding: 'ieee-p1363' }).toString('base64url');
  apTok = { t: head + '.' + body + '.' + sig, exp };
  return apTok.t;
}
app.get('/api/apple/token', (req, res) => {
  if (!appleOn()) return res.status(404).json({ error: 'not configured' });
  try { res.json({ token: appleToken() }); } catch (_) { res.status(500).json({ error: 'bad key' }); }
});
app.get('/api/img', async (req, res) => { // พร็อกซีรูปปก Apple Music (ให้อ่านสีจากปกได้)
  let u; try { u = new URL(String(req.query.u || '')); } catch (_) { return res.status(400).end(); }
  if (u.protocol !== 'https:' || !/\.mzstatic\.com$/.test(u.hostname)) return res.status(400).end();
  try {
    const r = await fetch(u, { signal: AbortSignal.timeout(6000) }), ct = r.headers.get('content-type') || '';
    if (!r.ok || !ct.startsWith('image/')) return res.status(404).end();
    const buf = Buffer.from(await r.arrayBuffer());
    if (buf.length > 3e6) return res.status(413).end();
    res.set({ 'Content-Type': ct, 'Cache-Control': 'public, max-age=86400' }).send(buf);
  } catch (_) { res.status(502).end(); }
});
function parseApple(link) {
  let u; try { u = new URL(link); } catch (_) { return null; }
  if (!/(^|\.)music\.apple\.com$/.test(u.hostname)) return null;
  const parts = u.pathname.split('/').filter(Boolean), sf = /^[a-z]{2}$/i.test(parts[0] || '') ? parts[0].toLowerCase() : 'th';
  const i = u.searchParams.get('i'), last = parts[parts.length - 1] || '';
  if (i && /^\d+$/.test(i)) return { kind: 'song', id: i, sf };
  if (parts.includes('song') && /^\d+$/.test(last)) return { kind: 'song', id: last, sf };
  if (parts.includes('playlist') && /^pl\.[\w-]+$/.test(last)) return { kind: 'playlist', id: last, sf };
  if (parts.includes('album') && /^\d+$/.test(last)) return { kind: 'album', id: last, sf };
  return null;
}
async function appleGet(pathq) {
  const r = await fetch('https://api.music.apple.com' + pathq, { headers: { Authorization: 'Bearer ' + appleToken() }, signal: AbortSignal.timeout(8000) });
  if (!r.ok) throw new Error('Apple Music ตอบกลับ ' + r.status);
  return r.json();
}
function toItem(d) {
  const a = (d && d.attributes) || {}, art = a.artwork && a.artwork.url ? String(a.artwork.url).replace('{w}', '600').replace('{h}', '600') : '';
  return {
    am: String(d.id), title: String(a.name || ''), artist: String(a.artistName || ''), art,
    info: [a.albumName, String(a.releaseDate || '').slice(0, 4), (a.genreNames || []).find((g) => g !== 'Music')].filter(Boolean).join(' · ') + (a.recordLabel ? '\n℗ ' + a.recordLabel : ''),
  };
}
async function appleItems(link) {
  const p = parseApple(link);
  if (!p) throw new Error('ลิงก์ Apple Music ไม่ถูกต้อง (ใช้ลิงก์เพลง อัลบั้ม หรือเพลย์ลิสต์จาก music.apple.com)');
  if (p.kind === 'song') return ((await appleGet('/v1/catalog/' + p.sf + '/songs/' + p.id)).data || []).map(toItem);
  let out = [], next = '/v1/catalog/' + p.sf + '/' + (p.kind === 'playlist' ? 'playlists' : 'albums') + '/' + p.id + '/tracks?limit=100';
  while (next && out.length < 300) {
    const j = await appleGet(next);
    out = out.concat((j.data || []).filter((d) => d.type === 'songs').map(toItem));
    next = j.next && String(j.next).startsWith('/') ? j.next : null;
  }
  return out.slice(0, 300);
}

// ---------- TikTok Live (ต่อใหม่อัตโนมัติ) ----------
const rooms = new Map();
const say = (room, text) => room.clients.forEach((c) => send(c, { type: 'status', text }));
const bcast = (room, o) => room.clients.forEach((c) => send(c, o));
function explain(e) {
  const n = (e && e.name) || '', m = (e && e.message) || String(e);
  if (/UserOffline/.test(n)) return 'แอคนี้ยังไม่ได้ไลฟ์ — เริ่มไลฟ์แล้วระบบจะต่อให้เอง';
  if (/InvalidUniqueId|HandleError/.test(n)) return 'ไม่พบชื่อแอคนี้ — ใช้ username (ชื่อหลัง @) ไม่ใช่ชื่อที่แสดง';
  if (/RateLimit/.test(n)) return 'ถูกจำกัดการเชื่อมต่อชั่วคราว — กำลังลองใหม่';
  if (/SignAPI|Signature/.test(n)) return 'ระบบเซ็นคำขอของไลบรารีใช้ไม่ได้: ' + m;
  if (/Timeout/.test(n)) return 'หมดเวลารอ TikTok — กำลังลองใหม่';
  return 'เชื่อมต่อไม่ได้: ' + n + ' ' + m;
}
const first = (img) => (img && (img.url || (img.urlList && img.urlList[0]) || (img.urls && img.urls[0]))) || '';
const info = (d) => {
  const u = d.user || {};
  const avatar = first(u.avatarThumb) || first(u.avatarMedium) || first(u.avatarLarge) || first(u.profilePicture) || first(u.profilePictureMedium) || d.profilePictureUrl || '';
  return { user: u.nickname || u.displayId || u.uniqueId || d.nickname || '?', id: u.uniqueId || u.displayId || d.uniqueId || '', avatar };
};
function connectRoom(room) {
  if (room.closed) return;
  const conn = new TikTokLiveConnection(room.username, process.env.SIGN_API_KEY ? { signApiKey: process.env.SIGN_API_KEY } : {});
  room.conn = conn;
  conn.on(WebcastEvent.CHAT, (d) => bcast(room, { type: 'chat', ...info(d), comment: d.comment || d.content || '' }));
  conn.on(WebcastEvent.LIKE, (d) => {
    const i = info(d), k = i.id || i.user, e = room.likes.get(k) || { ...i, count: 0 };
    e.count += Math.max(1, Math.min(Number(d.count) || 1, 100)); room.likes.set(k, e);
  });
  conn.on(WebcastEvent.FOLLOW, (d) => bcast(room, { type: 'follow', ...info(d) }));
  conn.on(WebcastEvent.GIFT, (d) => {
    const g = d.gift || {}, streaking = g.type === 1 && !d.repeatEnd;
    const cnt = Math.max(1, Number(d.repeatCount) || Number(d.comboCount) || 1);
    bcast(room, { type: 'gift', ...info(d), name: String(g.name || ''), diamonds: (Number(g.diamondCount) || 0) * cnt, streaking });
  });
  conn.on(WebcastEvent.SHARE, (d) => bcast(room, { type: 'share', ...info(d) }));
  conn.on(WebcastEvent.SOCIAL, (d) => { if (/share/i.test(JSON.stringify(d.displayType || d.label || ''))) bcast(room, { type: 'share', ...info(d) }); });
  conn.on('disconnected', () => { room.connected = false; say(room, 'หลุดจาก TikTok — กำลังต่อใหม่…'); retry(room); });
  conn.connect()
    .then(() => { room.tries = 0; room.connected = true; say(room, 'เชื่อมต่อ @' + room.username + ' แล้ว ✅'); })
    .catch((e) => { console.error('connect fail', room.username, e && e.name, e && e.message); say(room, explain(e)); retry(room, e); });
}
function retry(room, e) {
  if (room.closed || room.retrying) return;
  const n = (e && e.name) || '';
  if (/InvalidUniqueId|HandleError/.test(n)) return destroyRoom(room);
  const offline = /UserOffline/.test(n);
  if (!offline && ++room.tries > 8) { say(room, 'ต่อ TikTok ไม่สำเร็จหลายครั้ง — กดเชื่อมต่อใหม่'); return destroyRoom(room); }
  room.retrying = true;
  room.t = setTimeout(() => {
    room.retrying = false;
    try { room.conn.removeAllListeners(); room.conn.disconnect(); } catch (_) {}
    connectRoom(room);
  }, offline ? 12000 : Math.min(30000, 3000 * room.tries));
}
function destroyRoom(room) {
  room.closed = true; clearTimeout(room.t); clearInterval(room.likeT);
  try { room.conn && room.conn.removeAllListeners(); room.conn && room.conn.disconnect(); } catch (_) {}
  rooms.delete(room.username);
}
function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  room.clients.delete(ws);
  if (!room.clients.size) destroyRoom(room);
  ws.room = null;
}
function join(ws, username) {
  username = String(username || '').replace(/^@/, '').trim();
  if (!/^[\w.]{1,40}$/.test(username)) return send(ws, { type: 'status', text: 'ชื่อ TikTok ไม่ถูกต้อง' });
  if (ws.room && ws.room !== username) leave(ws);
  let room = rooms.get(username);
  if (!room) { room = { username, clients: new Set(), conn: null, tries: 0, closed: false, retrying: false, connected: false, t: null, likes: new Map(), likeT: null }; rooms.set(username, room); connectRoom(room); room.likeT = setInterval(() => { if (room.likes.size) { bcast(room, { type: 'likes', items: [...room.likes.values()] }); room.likes.clear(); } }, 400); }
  room.clients.add(ws); ws.room = username;
  send(ws, { type: 'status', text: room.connected ? 'เชื่อมต่อ @' + username + ' แล้ว ✅' : 'กำลังเชื่อมต่อ @' + username + '...' });
}

// ---------- เซสชัน: หน้าเกม (host) กับหน้าจัดการเพลง (admin) ----------
const sessions = new Map();
function host(ws, d) {
  if (!lib.songs.length && d.lib && Array.isArray(d.lib.songs)) { // กู้คลังจากสำเนาในเครื่องพิธีกร (กันเซิร์ฟเวอร์รีสตาร์ต)
    const c = cleanSongs(d.lib.songs);
    if (c && c.length) { lib = { rev: lib.rev + 1, songs: c }; persist(); }
  }
  const want = /^\d{6}$/.test(d.code || '') ? d.code : '';
  let s = want && sessions.get(want);
  if (!s) {
    let code = want && !sessions.has(want) ? want : '';
    while (!code || sessions.has(code)) code = String(100000 + Math.floor(Math.random() * 900000));
    s = { code, host: null, admins: new Set(), timer: null };
    sessions.set(code, s);
  }
  clearTimeout(s.timer);
  s.host = ws; ws.sess = s; ws.isHost = true;
  send(ws, { type: 'session', code: s.code, lib, apple: appleOn() });
}
function admin(ws, code) {
  ws.tries = (ws.tries || 0) + 1;
  if (ws.tries > 6) return ws.close();
  const s = sessions.get(String(code || ''));
  if (!s) return send(ws, { type: 'adminError', text: 'ไม่พบรหัสนี้ — ต้องเปิดหน้าเกมค้างไว้ และดูรหัสจากหน้าจอเกม' });
  ws.sess = s; ws.isAdmin = true; s.admins.add(ws);
  send(ws, { type: 'library', lib, apple: appleOn() });
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});
  ws.on('message', (m) => {
    let d; try { d = JSON.parse(m); } catch (_) { return; }
    if (!d || typeof d !== 'object') return;
    switch (d.type) {
      case 'join': join(ws, d.username); break;
      case 'host': host(ws, d); break;
      case 'admin': admin(ws, d.code); break;
      case 'meta': { // แอดมินขอชื่อวิดีโอตอนนำเข้าเพลย์ลิสต์
        if (!ws.isAdmin) break;
        const ids = (Array.isArray(d.ids) ? d.ids : []).filter((x) => VID.test(x)).slice(0, 300);
        loadMeta(ids)
          .then(() => send(ws, { type: 'meta', rid: d.rid, items: ids.map((id) => ({ yt: id, ...(meta.get(id) || { title: '', author: '' }) })) }))
          .catch(() => send(ws, { type: 'meta', rid: d.rid, items: [] }));
        break;
      }
      case 'apple': { // แอดมินนำเข้าจากลิงก์ Apple Music
        if (!ws.isAdmin) break;
        if (!appleOn()) { send(ws, { type: 'appleItems', rid: d.rid, error: 'เซิร์ฟเวอร์ยังไม่ได้ตั้งค่า Apple Music' }); break; }
        appleItems(String(d.link || '').slice(0, 300))
          .then((items) => send(ws, { type: 'appleItems', rid: d.rid, items }))
          .catch((e) => send(ws, { type: 'appleItems', rid: d.rid, error: String((e && e.message) || e) }));
        break;
      }
      case 'libSet': {
        if (!ws.isAdmin) break;
        const c = cleanSongs(d.songs); if (!c) break;
        lib = { rev: lib.rev + 1, songs: c }; persist();
        send(ws, { type: 'saved', count: c.length });
        for (const s of sessions.values()) {
          if (s.host) send(s.host, { type: 'library', lib });
          s.admins.forEach((a) => { if (a !== ws) send(a, { type: 'library', lib, apple: appleOn() }); });
        }
        break;
      }
    }
  });
  ws.on('close', () => {
    const s = ws.sess;
    if (s) {
      s.admins.delete(ws);
      if (s.host === ws) { s.host = null; s.timer = setTimeout(() => sessions.delete(s.code), 30 * 60 * 1000); }
    }
    leave(ws);
  });
});

setInterval(() => wss.clients.forEach((c) => {
  if (!c.isAlive) return c.terminate();
  c.isAlive = false;
  try { c.ping(); } catch (_) {}
  send(c, { type: 'ping' });
}), 25000);

server.listen(process.env.PORT || 3000, () => console.log('up'));

const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

process.on('uncaughtException', (e) => console.error('uncaught', e));
process.on('unhandledRejection', (e) => console.error('unhandled', e));

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/admin', (_, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/healthz', (_, res) => res.send('ok'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
const send = (ws, o) => { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (_) {} } };

// ---------- คลังเพลง (พิมพ์เอง) ----------
const FILE = path.join(process.env.DATA_DIR || __dirname, 'songs.json');
let lib = { rev: 0, songs: [] };
try { const j = JSON.parse(fs.readFileSync(FILE, 'utf8')); if (j && Array.isArray(j.songs)) lib = j; } catch (_) {}
const persist = () => { const t = FILE + '.tmp'; fs.writeFile(t, JSON.stringify(lib), (e) => { if (!e) fs.rename(t, FILE, () => {}); }); };
const VID = /^[\w-]{11}$/;
function cleanSongs(arr) {
  if (!Array.isArray(arr)) return null;
  return arr.slice(0, 500).map((s) => {
    if (!s || typeof s !== 'object') return null;
    const yt = String(s.yt || ''), title = String(s.title || '').trim().slice(0, 120);
    if (!VID.test(yt) || !title) return null;
    const st = s.start === null || s.start === undefined || s.start === '' ? null : Number(s.start);
    return {
      id: String(s.id || yt).slice(0, 24), title, yt,
      artist: String(s.artist || '').trim().slice(0, 80),
      start: Number.isFinite(st) ? Math.max(0, Math.min(7200, st)) : null,
      hint: String(s.hint || '').slice(0, 600),
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
  room.closed = true; clearTimeout(room.t);
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
  if (!room) { room = { username, clients: new Set(), conn: null, tries: 0, closed: false, retrying: false, connected: false, t: null }; rooms.set(username, room); connectRoom(room); }
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
  send(ws, { type: 'session', code: s.code, lib });
}
function admin(ws, code) {
  ws.tries = (ws.tries || 0) + 1;
  if (ws.tries > 6) return ws.close();
  const s = sessions.get(String(code || ''));
  if (!s) return send(ws, { type: 'adminError', text: 'ไม่พบรหัสนี้ — ต้องเปิดหน้าเกมค้างไว้ และดูรหัสจากหน้าจอเกม' });
  ws.sess = s; ws.isAdmin = true; s.admins.add(ws);
  send(ws, { type: 'library', lib });
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
      case 'libSet': {
        if (!ws.isAdmin) break;
        const c = cleanSongs(d.songs); if (!c) break;
        lib = { rev: lib.rev + 1, songs: c }; persist();
        send(ws, { type: 'saved', count: c.length });
        for (const s of sessions.values()) {
          if (s.host) send(s.host, { type: 'library', lib });
          s.admins.forEach((a) => { if (a !== ws) send(a, { type: 'library', lib }); });
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

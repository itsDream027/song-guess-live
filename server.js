const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

const app = express();
app.use(express.static('public'));
app.get('/admin', (_, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/healthz', (_, res) => res.send('ok'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// หนึ่งห้องไลฟ์ต่อหนึ่ง username แชร์ให้ทุก client ที่เปิดอยู่
const rooms = new Map(); // username -> { conn, clients:Set }

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }

function explain(e) {
  const n = (e && e.name) || '', m = (e && e.message) || String(e);
  if (/UserOffline/.test(n)) return 'แอคนี้ยังไม่ได้ไลฟ์ — เริ่มไลฟ์ก่อน แล้วกดเริ่มเกมใหม่';
  if (/InvalidUniqueId|HandleError/.test(n)) return 'ไม่พบชื่อแอคนี้ — ใช้ username (ชื่อหลัง @) ไม่ใช่ชื่อที่แสดง';
  if (/RateLimit/.test(n)) return 'ถูกจำกัดการเชื่อมต่อชั่วคราว — รอ 1-2 นาทีแล้วลองใหม่';
  if (/SignAPI|Signature/.test(n)) return 'ระบบเซ็นคำขอของไลบรารีใช้ไม่ได้: ' + m;
  if (/Timeout/.test(n)) return 'หมดเวลารอ TikTok — ลองใหม่อีกครั้ง';
  return 'เชื่อมต่อไม่ได้: ' + n + ' ' + m;
}

function join(ws, username) {
  username = String(username || '').replace(/^@/, '').trim();
  if (!username) return send(ws, { type: 'status', text: 'ใส่ชื่อ TikTok ก่อน' });
  let room = rooms.get(username);
  if (!room) {
    const conn = new TikTokLiveConnection(username, process.env.SIGN_API_KEY ? { signApiKey: process.env.SIGN_API_KEY } : {});
    room = { conn, clients: new Set() };
    rooms.set(username, room);
    const say = (text) => room.clients.forEach((c) => send(c, { type: 'status', text }));
    const first = (img) => (img && (img.url || (img.urlList && img.urlList[0]) || (img.urls && img.urls[0]))) || '';
    const info = (d) => {
      const u = d.user || {};
      const avatar = first(u.avatarThumb) || first(u.avatarMedium) || first(u.avatarLarge) || first(u.profilePicture) || first(u.profilePictureMedium) || d.profilePictureUrl || '';
      return { user: u.nickname || u.displayId || u.uniqueId || d.nickname || '?', id: u.uniqueId || u.displayId || d.uniqueId || '', avatar };
    };
    const all = (obj) => room.clients.forEach((c) => send(c, obj));
    conn.on(WebcastEvent.CHAT, (d) => all({ type: 'chat', ...info(d), comment: d.comment || d.content || '' }));
    conn.on(WebcastEvent.SHARE, (d) => all({ type: 'share', ...info(d) }));
    conn.on(WebcastEvent.SOCIAL, (d) => { if (/share/i.test(JSON.stringify(d.displayType || d.label || ''))) all({ type: 'share', ...info(d) }); });
    conn.on('disconnected', () => { say('หลุดจาก TikTok Live'); rooms.delete(username); });
    conn.connect()
      .then(() => say('เชื่อมต่อ @' + username + ' แล้ว ✅'))
      .catch((e) => { console.error('connect fail', username, e && e.name, e && e.message); say(explain(e)); rooms.delete(username); });
  }
  room.clients.add(ws);
  ws.room = username;
  send(ws, { type: 'status', text: 'กำลังเชื่อมต่อ @' + username + '...' });
}

// ---------- จัดการเพลง: เฉลยที่แก้ + เซสชันสำหรับหน้า admin ----------
const FILE = path.join(__dirname, 'answers.json');
let answers = {}; // videoId -> เฉลยที่แก้ไว้
try { answers = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (_) {}
const save = () => fs.writeFile(FILE, JSON.stringify(answers), () => {});
const meta = new Map();     // videoId -> { title, author }
const sessions = new Map(); // code -> { code, host, admins:Set, songs, played, loading, timer }
const VID = /^[\w-]{11}$/;

async function loadMeta(ids) {
  const todo = ids.filter((id) => !meta.has(id));
  for (let i = 0; i < todo.length; i += 6) {
    await Promise.all(todo.slice(i, i + 6).map(async (id) => {
      try {
        const r = await fetch('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent('https://www.youtube.com/watch?v=' + id), { signal: AbortSignal.timeout(6000) });
        const j = await r.json();
        meta.set(id, { title: j.title || '', author: j.author_name || '' });
      } catch (_) {}
    }));
  }
}
const buildSongs = (ids) => ids.map((id) => ({ id, ...(meta.get(id) || { title: '', author: '' }) }));
const listMsg = (s) => ({ type: 'songlist', songs: s.songs, answers, played: s.played, loading: s.loading });
const push = (s) => s.admins.forEach((a) => send(a, listMsg(s)));

function host(ws, d) {
  if (d.answers && typeof d.answers === 'object') { // กู้เฉลยที่เก็บไว้ในเครื่องพิธีกร (กันเซิร์ฟเวอร์รีสตาร์ต)
    for (const [k, v] of Object.entries(d.answers)) if (VID.test(k) && typeof v === 'string' && v.trim() && !(k in answers)) answers[k] = v.trim().slice(0, 120);
    save();
  }
  let s = /^\d{6}$/.test(d.code || '') ? sessions.get(d.code) : null;
  if (!s) {
    let code = /^\d{6}$/.test(d.code || '') && !sessions.has(d.code) ? d.code : '';
    while (!code || sessions.has(code)) code = String(100000 + Math.floor(Math.random() * 900000));
    s = { code, host: null, admins: new Set(), songs: [], played: {}, loading: false, timer: null };
    sessions.set(code, s);
  }
  clearTimeout(s.timer);
  s.host = ws; ws.sess = s; ws.isHost = true;
  send(ws, { type: 'session', code: s.code, answers });
}

async function songs(s, ids) {
  ids = (Array.isArray(ids) ? ids : []).filter((x) => VID.test(x)).slice(0, 500);
  s.songs = buildSongs(ids); s.loading = true; push(s);
  await loadMeta(ids);
  s.songs = buildSongs(ids); s.loading = false; push(s);
}

function admin(ws, code) {
  ws.tries = (ws.tries || 0) + 1;
  if (ws.tries > 6) return ws.close();
  const s = sessions.get(String(code || ''));
  if (!s) return send(ws, { type: 'adminError', text: 'ไม่พบรหัสนี้ — ต้องเปิดหน้าเกมค้างไว้ และดูรหัสจากหน้าจอเกม' });
  ws.sess = s; ws.isAdmin = true; s.admins.add(ws);
  send(ws, listMsg(s));
}

wss.on('connection', (ws) => {
  ws.on('message', (m) => {
    let d; try { d = JSON.parse(m); } catch (_) { return; }
    const s = ws.sess;
    if (d.type === 'join') join(ws, d.username);
    else if (d.type === 'host') host(ws, d);
    else if (d.type === 'songs' && ws.isHost) songs(s, d.ids);
    else if (d.type === 'played' && ws.isHost && VID.test(d.id || '')) { s.played[d.id] = String(d.ans || '').slice(0, 120); push(s); }
    else if (d.type === 'admin') admin(ws, d.code);
    else if (d.type === 'setAnswer' && ws.isAdmin && VID.test(d.id || '')) {
      const v = String(d.answer || '').trim().slice(0, 120);
      if (v) answers[d.id] = v; else delete answers[d.id];
      save();
      const msg = { type: 'answer', id: d.id, answer: v };
      if (s.host) send(s.host, msg);
      s.admins.forEach((a) => send(a, msg));
    }
  });
  ws.on('close', () => {
    const s = ws.sess;
    if (s) {
      s.admins.delete(ws);
      if (s.host === ws) { s.host = null; s.timer = setTimeout(() => sessions.delete(s.code), 30 * 60 * 1000); }
    }
    const room = rooms.get(ws.room);
    if (!room) return;
    room.clients.delete(ws);
    if (!room.clients.size) { try { room.conn.disconnect(); } catch (_) {} rooms.delete(ws.room); }
  });
});

const ping = setInterval(() => wss.clients.forEach((c) => send(c, { type: 'ping' })), 25000); // กัน Render ตัดการเชื่อมต่อ
server.listen(process.env.PORT || 3000, () => console.log('up'));

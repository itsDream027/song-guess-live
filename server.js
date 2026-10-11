const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

const app = express();
app.use(express.static('public'));
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

wss.on('connection', (ws) => {
  ws.on('message', (m) => { try { const d = JSON.parse(m); if (d.type === 'join') join(ws, d.username); } catch (_) {} });
  ws.on('close', () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    room.clients.delete(ws);
    if (!room.clients.size) { try { room.conn.disconnect(); } catch (_) {} rooms.delete(ws.room); }
  });
});

const ping = setInterval(() => wss.clients.forEach((c) => send(c, { type: 'ping' })), 25000); // กัน Render ตัดการเชื่อมต่อ
server.listen(process.env.PORT || 3000, () => console.log('up'));

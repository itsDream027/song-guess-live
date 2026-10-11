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

function join(ws, username) {
  username = String(username || '').replace(/^@/, '').trim();
  if (!username) return send(ws, { type: 'status', text: 'ใส่ชื่อ TikTok ก่อน' });
  let room = rooms.get(username);
  if (!room) {
    const conn = new TikTokLiveConnection(username);
    room = { conn, clients: new Set() };
    rooms.set(username, room);
    const say = (text) => room.clients.forEach((c) => send(c, { type: 'status', text }));
    conn.on(WebcastEvent.CHAT, (d) => {
      const user = (d.user && (d.user.nickname || d.user.uniqueId)) || d.nickname || d.uniqueId || '?';
      room.clients.forEach((c) => send(c, { user, comment: d.comment || '' }));
    });
    conn.on('disconnected', () => { say('หลุดจาก TikTok Live'); rooms.delete(username); });
    conn.connect()
      .then(() => say('เชื่อมต่อ @' + username + ' แล้ว ✅'))
      .catch((e) => { say('เชื่อมต่อไม่ได้: ' + (e.message || e) + ' (ต้องกำลังไลฟ์อยู่)'); rooms.delete(username); });
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

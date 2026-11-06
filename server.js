// Pitch Rush relay server. Matches the protocol of hostRelay/joinRelay in the game.
// Host:   {k:'host'} -> {k:'code',code}
// Client: {k:'join',code} -> {k:'ok'} | {k:'none'}
// Client -> host: {k:'up',d}  => host gets {k:'up',c:clientId,d}
// Host -> clients: {k:'dn',to:'all'|clientId,d} => client gets {k:'dn',d}
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => { res.writeHead(200); res.end('pitch-rush relay ok'); });
const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 64 * 1024 });

const rooms = new Map(); // code -> { host: ws, clients: Map(id -> ws) }
let nextId = 1;

function newCode() {
  for (let i = 0; i < 200; i++) {
    const c = String(Math.floor(1000 + Math.random() * 9000));
    if (!rooms.has(c)) return c;
  }
  return null;
}
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws._net = null; // {role:'host',code} or {role:'client',code,id}
  if (ws._socket && ws._socket.setNoDelay) ws._socket.setNoDelay(true);

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const me = ws._net;

    if (!me) {
      if (m.k === 'host') {
        const code = newCode(); if (!code) return ws.close();
        rooms.set(code, { host: ws, clients: new Map() });
        ws._net = { role: 'host', code };
        send(ws, { k: 'code', code });
      } else if (m.k === 'join') {
        const room = rooms.get(String(m.code));
        if (!room) return send(ws, { k: 'none' });
        const id = 'c' + (nextId++);
        room.clients.set(id, ws);
        ws._net = { role: 'client', code: String(m.code), id };
        send(ws, { k: 'ok' });
      }
      return;
    }

    const room = rooms.get(me.code); if (!room) return;
    if (me.role === 'client' && m.k === 'up') {
      send(room.host, { k: 'up', c: me.id, d: m.d });
    } else if (me.role === 'host' && m.k === 'dn') {
      const out = JSON.stringify({ k: 'dn', d: m.d });
      if (m.to === 'all') { for (const c of room.clients.values()) if (c.readyState === 1) c.send(out); }
      else { const c = room.clients.get(m.to); if (c && c.readyState === 1) c.send(out); }
    }
  });

  ws.on('close', () => {
    const me = ws._net; if (!me) return;
    const room = rooms.get(me.code); if (!room) return;
    if (me.role === 'host') {
      for (const c of room.clients.values()) { send(c, { k: 'bye' }); c.close(); }
      rooms.delete(me.code);
    } else {
      room.clients.delete(me.id);
      send(room.host, { k: 'up', c: me.id, l: 1 });
    }
  });
  ws.on('error', () => {});
});

// drop dead connections
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 10000);

server.listen(PORT, () => console.log('relay listening on ' + PORT));

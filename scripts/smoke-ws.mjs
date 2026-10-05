// End-to-end check of the session handshake against a running server.
//   node scripts/smoke-ws.mjs            -> new session
//   node scripts/smoke-ws.mjs <uuid>     -> resume that session
// Streams 2.048 s of silence, then stops. Prints every server message.
const url = process.env.WS_URL ?? 'ws://127.0.0.1:8080/ws';
const sessionId = process.argv[2];

const ws = new WebSocket(url);
ws.binaryType = 'arraybuffer';

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'start',
    sampleRate: 16000,
    encoding: 'pcm_s16le',
    language: 'en',
    ...(sessionId ? { sessionId } : {}),
  }));
};

ws.onmessage = async (ev) => {
  const msg = JSON.parse(ev.data);
  console.log(JSON.stringify(msg));
  if (msg.type === 'ready') {
    const frame = new Uint8Array(4096); // 128 ms of 16 kHz Int16 silence
    for (let i = 0; i < 16; i++) {
      ws.send(frame);
      await new Promise((r) => setTimeout(r, 128));
    }
    ws.send(JSON.stringify({ type: 'stop' }));
  }
};

ws.onclose = (ev) => {
  console.log(`closed ${ev.code}`);
  process.exit(0);
};
ws.onerror = (ev) => {
  console.error('websocket error', ev.message ?? ev);
  process.exit(1);
};

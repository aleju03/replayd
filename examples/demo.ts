// Runnable end-to-end demo. Boots a real HTTP server with replayd's SSE handler,
// opens an EventSource-style client to one topic, publishes a few events (and
// watches them arrive live), then drops the connection, publishes more while
// "offline", and reconnects with Last-Event-ID to see the missed events replay.
//
//   npm run example
//
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { createDb, EventHub } from "../src/index.js";

interface SseFrame {
  id?: string;
  event: string;
  data: string;
}

async function main(): Promise<void> {
  const db = await createDb({ url: ":memory:" });
  const hub = new EventHub(db, {
    defaultType: "score",
    sse: { allowedOrigins: "*", heartbeatMs: 5_000 },
  });
  await hub.ensureSchema();

  const server = createServer((req, res) => {
    hub.handleSse(req, res).then((handled) => {
      if (!handled) {
        res.writeHead(404);
        res.end();
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  console.log(`replayd demo server listening on ${base}`);

  // 1) Open a live SSE stream to the "CR" topic (as a browser EventSource would).
  console.log("\n[1] client A connects to topic=CR and listens live");
  const clientA = await openSse(`${base}/events?topic=CR`, {}, (frame) => {
    if (frame.event === "score") console.log(`    live  -> id=${frame.id} ${frame.data}`);
  });
  await clientA.waitFor((f) => f.event === "hello");

  // 2) Publish a few events; client A sees them arrive.
  console.log("\n[2] publishing three CR events plus one broadcast");
  await hub.publish("CR", { player: "alice", pp: 512 });
  await hub.publish("CR", { player: "bob", pp: 498 });
  const lastSeen = await hub.publish("CR", { player: "carol", pp: 530 });
  await hub.publish(null, { notice: "server maintenance in 10m" }); // null topic = broadcast to all
  await clientA.waitFor((f) => f.event === "score" && f.data.includes("carol"));
  await delay(100);
  const cursor = lastSeen.sequence;
  console.log(`    client A's last live cursor (Last-Event-ID) is ${cursor}`);

  // 3) Client A drops. Events keep flowing while it is offline.
  console.log("\n[3] client A disconnects; two more CR events are published while it is offline");
  clientA.close();
  await hub.publish("CR", { player: "dave", pp: 505 });
  await hub.publish("US", { player: "erin", pp: 601 }); // different topic: not for CR
  await hub.publish("CR", { player: "frank", pp: 545 });

  // 4) Reconnect with Last-Event-ID: only the missed CR/broadcast events replay.
  console.log("\n[4] client A reconnects with Last-Event-ID and replays what it missed");
  const missed: string[] = [];
  const clientB = await openSse(`${base}/events?topic=CR`, { "last-event-id": String(cursor) }, (frame) => {
    if (frame.event === "score") {
      missed.push(frame.data);
      console.log(`    replay -> id=${frame.id} ${frame.data}`);
    }
  });
  await clientB.waitFor((f) => f.event === "score" && f.data.includes("frank"));
  await delay(100);
  clientB.close();

  console.log(`\n    replayed ${missed.length} missed events (the broadcast notice + dave + frank; the US-topic event was correctly skipped)`);

  // 5) A client that falls further behind than replayLimit cannot be handed its
  //    whole backlog. Rather than quietly skipping the middle, replayd sends a
  //    gap event naming the lost range and then jumps the client to the newest
  //    window, so it ends up current instead of stuck behind a backlog it can
  //    never drain. A second hub over the same db, with a deliberately tiny limit:
  console.log("\n[5] a client too far behind gets an explicit gap, not a silent hole");
  const strictHub = new EventHub(db, { defaultType: "score", sse: { allowedOrigins: "*", replayLimit: 2 } });
  const strictServer = createServer((req, res) => {
    strictHub.handleSse(req, res).then((handled) => {
      if (!handled) {
        res.writeHead(404);
        res.end();
      }
    });
  });
  await new Promise<void>((resolve) => strictServer.listen(0, resolve));
  const strictBase = `http://127.0.0.1:${(strictServer.address() as AddressInfo).port}`;

  const clientC = await openSse(`${strictBase}/events?topic=CR`, { "last-event-id": String(cursor) }, (frame) => {
    if (frame.event === "gap") console.log(`    gap    -> ${frame.data}`);
    if (frame.event === "score") console.log(`    recent -> id=${frame.id} ${frame.data}`);
  });
  await clientC.waitFor((f) => f.event === "gap");
  await delay(100);
  clientC.close();
  strictServer.close();
  strictServer.closeAllConnections();

  // 6) Retention: the log is append-only, so something has to bound it. Prune by
  //    count and/or age, once or on an interval via startRetention().
  console.log("\n[6] retention keeps the log bounded");
  console.log(`    events before prune: ${(await hub.replay(null, 0, 1000)).length}`);
  const dropped = await hub.prune({ maxEvents: 3 });
  console.log(`    pruned ${dropped}; events after: ${(await hub.replay(null, 0, 1000)).length}`);

  // 7) Hub stats: the durable head and any tracked topic clients.
  const stats = await hub.stats();
  console.log("\n[7] hub stats:", JSON.stringify(stats));

  server.close();
  server.closeAllConnections();
  db.close();
  console.log("\nok");
}

interface DemoClient {
  waitFor(pred: (f: SseFrame) => boolean, timeoutMs?: number): Promise<SseFrame>;
  close(): void;
}

async function openSse(
  url: string,
  headers: Record<string, string>,
  onFrame: (frame: SseFrame) => void,
): Promise<DemoClient> {
  const controller = new AbortController();
  const res = await fetch(url, { headers: { accept: "text/event-stream", ...headers }, signal: controller.signal });
  if (!res.ok || !res.body) throw new Error(`sse connect failed: ${res.status}`);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const frames: SseFrame[] = [];
  const waiters: Array<{ pred: (f: SseFrame) => boolean; resolve: (f: SseFrame) => void }> = [];
  let buffer = "";

  void (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const raw = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const frame = parseFrame(raw);
        frames.push(frame);
        onFrame(frame);
        for (let i = waiters.length - 1; i >= 0; i -= 1) {
          if (waiters[i].pred(frame)) {
            waiters[i].resolve(frame);
            waiters.splice(i, 1);
          }
        }
      }
    }
  })().catch(() => undefined);

  return {
    waitFor(pred, timeoutMs = 3_000) {
      const existing = frames.find(pred);
      if (existing) return Promise.resolve(existing);
      return new Promise<SseFrame>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("waitFor timed out")), timeoutMs);
        waiters.push({ pred, resolve: (frame) => { clearTimeout(timer); resolve(frame); } });
      });
    },
    close() {
      controller.abort();
    },
  };
}

function parseFrame(raw: string): SseFrame {
  let id: string | undefined;
  let event = "message";
  const dataLines: string[] = [];
  for (const line of raw.split("\n")) {
    if (line.startsWith("id:")) id = line.slice(3).trim();
    else if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
  }
  return { id, event, data: dataLines.join("\n") };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

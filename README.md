<p align="center">
  <img src="assets/mascot.png" alt="replayd mascot" height="160">
</p>

# replayd

Resumable Server-Sent Events on SQLite. A durable event log, **Last-Event-ID replay on reconnect**, and per-topic client tracking.

"replayd" = replay daemon. It is the small piece you keep when you want browser SSE that survives a dropped connection: every event is appended to one SQLite table with a monotonic sequence, that sequence is the SSE `id`, and when a client reconnects with its `Last-Event-ID` it gets exactly the events it missed, in order, before the live stream resumes. You bring the payloads and the HTTP server.

## Why

- **Reconnects do not lose events.** The browser `EventSource` sends `Last-Event-ID` automatically on reconnect. replayd resumes from that sequence: hello frame, replay the gap, then live. No client bookkeeping.
- **One SQLite file.** The log is an append-only table with an autoincrement `sequence` and a unique `event_id` (append is idempotent by that id). No broker, no Redis, no per-message pricing.
- **Topics, with a broadcast lane.** Publish to a topic and only that topic's subscribers see it. Publish with `topic: null` and every subscriber sees it. Subscribe with no topic to receive all topics (a global aggregate).
- **Server/worker split.** One process can append (a worker) while another polls the table with `startTail()` and fans new rows out to its own SSE connections, so a write on any connection or process reaches every reader exactly once.

## Install

```bash
npm install @aleju03/replayd
```

Requires Node 18+ (the SSE handler and examples use global `fetch` / `http`). Storage is `@libsql/client`, so a `file:` path, `:memory:`, or a remote Turso URL all work. Use a `file:` URL if you want replay to survive a restart.

## Quick start

```ts
import { EventHub, createDb } from "@aleju03/replayd";
import { createServer } from "node:http";

const db = await createDb({ url: "file:events.db" });
const hub = new EventHub(db, {
  defaultType: "message",              // SSE event type when publish() is not given one
  sse: { path: "/events", allowedOrigins: ["https://app.example.com"] },
});
await hub.ensureSchema();

// Mount the SSE endpoint. handleSse returns true when it handled the request.
createServer((req, res) => {
  hub.handleSse(req, res).then((handled) => { if (!handled) { res.writeHead(404); res.end(); } });
}).listen(3000);

// Publish from anywhere: append to the durable log and fan out to live clients.
await hub.publish("room-42", { text: "hello" });          // only room-42 subscribers
await hub.publish(null, { notice: "maintenance in 10m" }); // broadcast to everyone
```

In the browser it is just `EventSource`. Reconnect and replay are handled by the platform and the server:

```js
const es = new EventSource("https://app.example.com/events?topic=room-42");
es.addEventListener("message", (e) => {
  console.log(e.lastEventId, JSON.parse(e.data));
});
// On a dropped connection the browser reconnects automatically, sending
// Last-Event-ID; replayd replays the events published while it was gone.
```

## The resume story

Each event carries the SSE frame:

```
id: 128
event: message
data: {"text":"hello"}
```

The `id` is the row's `sequence`. When a client connects:

1. A `hello` frame reports the resume sequence.
2. If the request carries `Last-Event-ID` (a header the browser sets automatically, or `?lastEventId=` for manual clients), replayd calls `replay(topic, sinceSequence)` and writes every missed event, in `sequence` order, capped at `replayLimit` (default 100).
3. Then the client is subscribed to the live fan-out.

Heartbeats (default every 15s) are sent with **no `id` line on purpose**: an id on a heartbeat would overwrite the browser's `Last-Event-ID` cursor with a heartbeat marker and break replay on the next reconnect. Heartbeats carry liveness only, never a resume point. Pass an optional `heartbeatData` hook to attach your own freshness signal to each one.

`handleSse` options: `path` (default `/events`), `topicParam` (default `topic`), `allowedOrigins` (`string[] | "*"`), `replayLimit` (default 100), `heartbeatMs` (default 15000), and `heartbeatData(topic)`.

## Server/worker split with startTail

When ingest runs in a different process from the one serving SSE, the serving process cannot dispatch in memory (the append happened elsewhere). Instead it tails the log:

```ts
// worker process: appends only, never serves SSE
const worker = new EventHub(db);
await worker.publish("room-42", { text: "from the worker" });

// server process: serves SSE, tails the table for rows written by the worker
const server = new EventHub(db);
const stop = server.startTail(250); // poll every 250ms, forward new rows to SSE clients
```

`startTail` pins its cursor at the current head, so it forwards only events created after it starts (reconnecting clients get their own backlog via replay-on-connect, not the tailer). While a hub is tailing, `publish()` skips its in-process dispatch so the poller stays the single delivery path and nothing is delivered twice. Call the returned function to stop.

## API

Everything is reachable through the `EventHub` facade, or use the pieces directly.

```ts
hub.ensureSchema();                       // create the table + index (once)
hub.publish(topic, payload, opts?);       // append + fan out; opts: { type?, eventId? }
hub.subscribe((event) => { ... });        // in-process live sink; returns an unsubscribe fn
hub.replay(topic, sinceSequence, limit?); // the events after a sequence (topic + broadcasts)
hub.latestSequence();                     // current head
hub.startTail(intervalMs);                // server/worker poller; returns a stop fn
hub.stats();                              // { latestSequence, topics: [{ topic, activeClients, lastActiveAt }] }
hub.handleSse(req, res);                  // the bound SSE endpoint

hub.log;      // the underlying EventLog
hub.clients;  // the underlying TopicClientTracker
```

`publish` is idempotent by `eventId`: a retried publish with the same id neither duplicates the row nor re-delivers to live clients. Omit it and a stable id is generated.

A `ReplayEvent` is `{ sequence, event_id, type, topic, payload, created_at }`.

## Run the demo

```bash
npm install
npm run example   # boots a server, streams live events, then reconnects and replays the gap
npm test          # unit + end-to-end HTTP/SSE tests (including the resume path)
```

## License

MIT

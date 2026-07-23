<p align="center">
  <img src="assets/mascot.png" alt="replayd mascot" height="160">
</p>

# replayd

Resumable Server-Sent Events on SQLite. Every event appends to a log table whose row sequence becomes the SSE `id`, so a browser that reconnects with `Last-Event-ID` gets exactly the events it missed, in order, before the live stream resumes.

The name is "replay daemon". It's the small piece you keep when you want browser SSE that survives a dropped connection without putting a broker in the middle. You bring the payloads and the HTTP server; replayd owns the log, the replay, and the per-topic fan-out.

## What you actually get

The browser's `EventSource` already does the hard part: it reconnects on its own and sends `Last-Event-ID` when it does. Most servers throw that header away. replayd treats it as a cursor into a durable log, which turns the platform's built-in reconnect into real resume semantics with zero client bookkeeping.

The log is one append-only SQLite table with an autoincrement `sequence` and a unique `event_id`, so appends are idempotent and there's no Redis in the picture, and no per-message pricing either.

Topics scope delivery. Publish to `"room-42"` and only that topic's subscribers see it; publish to `null` and everyone does. A subscriber with no topic gets the global aggregate.

When ingest and serving live in different processes, `startTail()` lets the serving process poll the table and fan out rows a worker wrote elsewhere. A write from any process reaches every reader exactly once.

## Install

```bash
npm install @aleju03/replayd
```

Requires Node 18+ (the SSE handler and examples use global `fetch` / `http`). Storage is `@libsql/client`, which means a `file:` path, `:memory:`, or a remote Turso URL all work. Use a `file:` URL if replay should survive a restart.

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

In the browser it's just `EventSource`; reconnect and replay are handled by the platform and the server:

```js
const es = new EventSource("https://app.example.com/events?topic=room-42");
es.addEventListener("message", (e) => {
  console.log(e.lastEventId, JSON.parse(e.data));
});
// On a dropped connection the browser reconnects automatically, sending
// Last-Event-ID; replayd replays the events published while it was gone.
```

## The resume story

Each event goes out as an SSE frame:

```
id: 128
event: message
data: {"text":"hello"}
```

The `id` is the row's `sequence`. When a client connects:

1. A `hello` frame reports the resume sequence.
2. If the request carries `Last-Event-ID` (a header the browser sets automatically, or `?lastEventId=` for manual clients), replayd calls `replay(topic, sinceSequence)` and writes every missed event, in `sequence` order, capped at `replayLimit` (default 100).
3. Then the client joins the live fan-out.

Heartbeats (default every 15s) are sent with no `id` line, and that's deliberate: an id on a heartbeat would overwrite the browser's `Last-Event-ID` cursor with a heartbeat marker and quietly break replay on the next reconnect. Heartbeats carry liveness only, never a resume point. Pass an optional `heartbeatData` hook to attach your own freshness signal to each one.

`handleSse` options: `path` (default `/events`), `topicParam` (default `topic`), `allowedOrigins` (`string[] | "*"`), `replayLimit` (default 100), `heartbeatMs` (default 15000), and `heartbeatData(topic)`.

## Server/worker split with startTail

When ingest runs in a different process from the one serving SSE, the serving process can't dispatch in memory because the append happened elsewhere. So it tails the log instead:

```ts
// worker process: appends only, never serves SSE
const worker = new EventHub(db);
await worker.publish("room-42", { text: "from the worker" });

// server process: serves SSE, tails the table for rows written by the worker
const server = new EventHub(db);
const stop = server.startTail(250); // poll every 250ms, forward new rows to SSE clients
```

`startTail` pins its cursor at the current head, so it forwards only events created after it starts; reconnecting clients get their backlog via replay-on-connect, not from the tailer. While a hub is tailing, `publish()` skips its in-process dispatch so the poller stays the single delivery path and nothing arrives twice. Call the returned function to stop.

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

`publish` is idempotent by `eventId`: a retried publish with the same id neither duplicates the row nor re-delivers to live clients. Omit it and a stable id is generated for you.

A `ReplayEvent` is `{ sequence, event_id, type, topic, payload, created_at }`.

## Run the demo

```bash
npm install
npm run example   # boots a server, streams live events, then reconnects and replays the gap
npm test          # unit + end-to-end HTTP/SSE tests (including the resume path)
```

## License

MIT

import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { createDb, EventHub, type EventHubOptions } from "../src/index.js";

// End-to-end over a real http.Server + fetch: a connected client receives a
// live event, and a reconnecting client replays what it missed via Last-Event-ID.

interface SseFrame {
  id?: string;
  retry?: string;
  event: string;
  data: string;
}

interface SseClient {
  waitFor(pred: (f: SseFrame) => boolean, timeoutMs?: number): Promise<SseFrame>;
  /** Every frame received so far; used to assert something was NOT sent. */
  all(): SseFrame[];
  close(): void;
}

async function openSse(url: string, headers: Record<string, string> = {}): Promise<SseClient> {
  const controller = new AbortController();
  const res = await fetch(url, {
    headers: { accept: "text/event-stream", ...headers },
    signal: controller.signal,
  });
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
        const timer = setTimeout(() => reject(new Error("sse waitFor timed out")), timeoutMs);
        waiters.push({ pred, resolve: (frame) => { clearTimeout(timer); resolve(frame); } });
      });
    },
    all() {
      return [...frames];
    },
    close() {
      controller.abort();
    },
  };
}

function parseFrame(raw: string): SseFrame {
  let id: string | undefined;
  let retry: string | undefined;
  let event = "message";
  const dataLines: string[] = [];
  for (const line of raw.split("\n")) {
    if (line.startsWith("id:")) id = line.slice(3).trim();
    else if (line.startsWith("retry:")) retry = line.slice(6).trim();
    else if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
  }
  return { id, retry, event, data: dataLines.join("\n") };
}

async function startHub(sseOverrides: NonNullable<EventHubOptions["sse"]> = {}): Promise<{ hub: EventHub; base: string; server: Server }> {
  const db = await createDb({ url: ":memory:" });
  const hub = new EventHub(db, { defaultType: "ping", sse: { allowedOrigins: "*", heartbeatMs: 1_000, ...sseOverrides } });
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
  return { hub, base, server };
}

describe("sse handler", () => {
  const servers: Server[] = [];
  afterEach(async () => {
    await Promise.all(servers.map((s) => new Promise<void>((resolve) => {
      s.close(() => resolve());
      // Force-close lingering keep-alive sockets: an aborted fetch leaves the
      // socket pooled client-side, so close() would otherwise wait for the
      // server idle timeout before its callback fires.
      s.closeAllConnections();
    })));
    servers.length = 0;
  });

  it("delivers a live event to a connected client", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);

    const client = await openSse(`${base}/events?topic=alpha`);
    await client.waitFor((f) => f.event === "hello");

    await hub.publish("alpha", { hi: 1 });
    const frame = await client.waitFor((f) => f.event === "ping");
    expect(JSON.parse(frame.data)).toMatchObject({ hi: 1 });
    // The id line carries the resume cursor (the event sequence).
    expect(Number(frame.id)).toBeGreaterThan(0);

    // A different topic must not reach this client's stream.
    await hub.publish("beta", { hi: 2 });
    await hub.publish("alpha", { hi: 3 });
    const next = await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).hi === 3);
    expect(JSON.parse(next.data)).toMatchObject({ hi: 3 });

    client.close();
  });

  it("replays missed events on reconnect via Last-Event-ID", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);

    const first = await openSse(`${base}/events?topic=alpha`);
    await first.waitFor((f) => f.event === "hello");
    await hub.publish("alpha", { n: 1 });
    const seen = await first.waitFor((f) => f.event === "ping");
    const lastId = seen.id!;
    expect(Number(lastId)).toBeGreaterThan(0);
    first.close();

    // Events published while the client was disconnected.
    await hub.publish("alpha", { n: 2 });
    await hub.publish("beta", { n: 99 }); // other topic: must NOT be replayed to alpha
    await hub.publish(null, { n: 3 }); // broadcast (null topic): must be replayed to everyone

    // Reconnect with the Last-Event-ID header; the missed events replay in order.
    const resumed = await openSse(`${base}/events?topic=alpha`, { "last-event-id": lastId });
    const replayedTwo = await resumed.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 2);
    const replayedThree = await resumed.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 3);
    expect(JSON.parse(replayedTwo.data)).toMatchObject({ n: 2 });
    expect(JSON.parse(replayedThree.data)).toMatchObject({ n: 3 });

    // The other topic's event was skipped: everything replayed is n 2 or 3.
    const replayedNs = [replayedTwo, replayedThree].map((f) => JSON.parse(f.data).n);
    expect(replayedNs).not.toContain(99);

    resumed.close();
  });

  it("supports ?lastEventId= as an alternative to the header", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);

    const e1 = await hub.publish("alpha", { n: 1 });
    await hub.publish("alpha", { n: 2 });

    const client = await openSse(`${base}/events?topic=alpha&lastEventId=${e1.sequence}`);
    await client.waitFor((f) => f.event === "hello");
    const replayed = await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 2);
    expect(JSON.parse(replayed.data)).toMatchObject({ n: 2 });

    client.close();
  });

  it("delivers a live broadcast (null topic) to a topic-scoped client", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);

    const client = await openSse(`${base}/events?topic=alpha`);
    await client.waitFor((f) => f.event === "hello");

    // A null-topic publish is a broadcast: it must reach a client scoped to a
    // specific topic, live (not just on replay).
    await hub.publish(null, { broadcast: true });
    const frame = await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).broadcast === true);
    expect(JSON.parse(frame.data)).toMatchObject({ broadcast: true });

    client.close();
  });

  it("heartbeat frames carry liveness only, never an id line", async () => {
    const { base, server } = await startHub({ heartbeatMs: 30, heartbeatData: () => ({ fresh: true }) });
    servers.push(server);

    const client = await openSse(`${base}/events?topic=alpha`);
    const hb = await client.waitFor((f) => f.event === "heartbeat");
    // An id on a heartbeat would clobber the browser's Last-Event-ID cursor and
    // silently break replay on the next reconnect. It must be absent.
    expect(hb.id).toBeUndefined();
    // The heartbeatData hook is merged into the frame payload.
    expect(JSON.parse(hb.data)).toMatchObject({ fresh: true });

    client.close();
  });

  it("enforces allowedOrigins", async () => {
    const { base, server } = await startHub({ allowedOrigins: ["https://good.example"] });
    servers.push(server);

    const denied = await fetch(`${base}/events?topic=alpha`, {
      headers: { accept: "text/event-stream", origin: "https://evil.example" },
    });
    expect(denied.status).toBe(403);
    await denied.text(); // drain

    const controller = new AbortController();
    const allowed = await fetch(`${base}/events?topic=alpha`, {
      headers: { accept: "text/event-stream", origin: "https://good.example" },
      signal: controller.signal,
    });
    expect(allowed.status).toBe(200);
    expect(allowed.headers.get("access-control-allow-origin")).toBe("https://good.example");
    controller.abort();
  });

  it("tracks active clients per topic", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);

    const client = await openSse(`${base}/events?topic=gamma`);
    await client.waitFor((f) => f.event === "hello");
    const stats = await hub.stats();
    expect(stats.topics.find((t) => t.topic === "gamma")?.activeClients).toBe(1);

    client.close();
  });

  it("does not drop an event published during the connect handshake", async () => {
    const { hub, base, server } = await startHub();
    servers.push(server);
    const seed = await hub.publish("alpha", { n: 1 });

    // Let the replay query READ the log, then stall before it returns. An event
    // published in that window is already too late for the replay result and
    // would predate the live subscription, so it is the one the old ordering
    // lost: the handler must subscribe before replaying and buffer it.
    let replayRan!: () => void;
    const ran = new Promise<void>((resolve) => { replayRan = resolve; });
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    const realReplay = hub.log.replay.bind(hub.log);
    hub.log.replay = async (topic, since, limit) => {
      const result = await realReplay(topic, since, limit);
      replayRan();
      await held;
      return result;
    };

    const client = await openSse(`${base}/events?topic=alpha`, { "last-event-id": String(seed.sequence) });
    await ran;
    await hub.publish("alpha", { n: 2 });
    release();

    const frame = await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 2);
    expect(JSON.parse(frame.data)).toMatchObject({ n: 2 });
    // Delivered exactly once: replay and the buffered live copy overlap here.
    const twos = client.all().filter((f) => f.event === "ping" && JSON.parse(f.data).n === 2);
    expect(twos).toHaveLength(1);

    client.close();
  });

  it("emits a gap event and the newest window when the backlog outgrows replayLimit", async () => {
    const { hub, base, server } = await startHub({ replayLimit: 2 });
    servers.push(server);

    const seed = await hub.publish("alpha", { n: 0 });
    for (let n = 1; n <= 5; n += 1) await hub.publish("alpha", { n });

    const client = await openSse(`${base}/events?topic=alpha`, { "last-event-id": String(seed.sequence) });
    const gap = await client.waitFor((f) => f.event === "gap");
    // The gap frame is a notice, not a resume point: no id line.
    expect(gap.id).toBeUndefined();
    expect(JSON.parse(gap.data)).toMatchObject({
      resumedFrom: seed.sequence,
      missedFrom: seed.sequence + 1,
      missed: 3,
      replayLimit: 2,
    });

    // The client is handed the two NEWEST events, not the two oldest: it ends up
    // current, and its cursor no longer sits behind an undrainable backlog.
    await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 5);
    const delivered = client.all().filter((f) => f.event === "ping").map((f) => JSON.parse(f.data).n);
    expect(delivered).toEqual([4, 5]);

    client.close();
  });

  it("replays without a gap event when the backlog exactly fills replayLimit", async () => {
    const { hub, base, server } = await startHub({ replayLimit: 3 });
    servers.push(server);

    const seed = await hub.publish("alpha", { n: 0 });
    for (let n = 1; n <= 3; n += 1) await hub.publish("alpha", { n });

    const client = await openSse(`${base}/events?topic=alpha`, { "last-event-id": String(seed.sequence) });
    await client.waitFor((f) => f.event === "ping" && JSON.parse(f.data).n === 3);
    const delivered = client.all().filter((f) => f.event === "ping").map((f) => JSON.parse(f.data).n);
    expect(delivered).toEqual([1, 2, 3]);
    expect(client.all().some((f) => f.event === "gap")).toBe(false);

    client.close();
  });

  it("sends a retry hint when retryMs is set", async () => {
    const { base, server } = await startHub({ retryMs: 500 });
    servers.push(server);

    const client = await openSse(`${base}/events?topic=alpha`);
    const retry = await client.waitFor((f) => f.retry != null);
    expect(retry.retry).toBe("500");

    client.close();
  });
});

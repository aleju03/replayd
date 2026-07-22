import type { IncomingMessage, ServerResponse } from "node:http";
import type { EventLog, ReplayEvent } from "./event-log.js";
import type { TopicClientTracker } from "./topic-clients.js";

// A generic, framework-free Server-Sent Events handler. On connect it sends a
// hello frame carrying the resume sequence, replays any events the client
// missed (from its Last-Event-ID), then subscribes it to the live fan-out with
// per-topic filtering. Drop it in front of any http.Server; it returns true
// when it handled the request (its path matched) and false otherwise.

export interface HandleSseOptions {
  /** The durable log to resume from and subscribe to. */
  log: EventLog;
  /** Optional per-topic connection counter; open()/release() are called around the stream. */
  clients?: TopicClientTracker;
  /** Path this handler answers on (default "/events"). */
  path?: string;
  /** Query param naming the topic (default "topic"). Absent/blank means all topics. */
  topicParam?: string;
  /** Origins allowed to open the feed. "*" allows any; omit to skip the check. */
  allowedOrigins?: string[] | "*";
  /** Max events replayed on reconnect (default 100). */
  replayLimit?: number;
  /** Heartbeat interval in ms (default 15000). */
  heartbeatMs?: number;
  /** Optional hook whose result is merged into each heartbeat frame (e.g. a
   *  freshness signal read from your own store). Receives the stream's topic. */
  heartbeatData?: (topic: string | null) => unknown | Promise<unknown>;
}

export async function handleSse(req: IncomingMessage, res: ServerResponse, opts: HandleSseOptions): Promise<boolean> {
  const path = opts.path ?? "/events";
  const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
  if (url.pathname !== path) return false;

  const origin = req.headers.origin;
  if (origin && opts.allowedOrigins && opts.allowedOrigins !== "*" && !opts.allowedOrigins.includes(origin)) {
    res.statusCode = 403;
    res.end("forbidden");
    return true;
  }
  if (origin) {
    res.setHeader("access-control-allow-origin", opts.allowedOrigins === "*" ? "*" : origin);
    res.setHeader("vary", "origin");
  }

  const topicParam = opts.topicParam ?? "topic";
  const rawTopic = url.searchParams.get(topicParam);
  // No topic (or a blank one) means the global aggregate: this client receives
  // events from every topic. A set topic receives that topic plus broadcasts.
  const topic = rawTopic != null && rawTopic.trim() !== "" ? rawTopic.trim() : null;

  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
  });

  const cursorRaw = firstHeader(req.headers["last-event-id"]) ?? url.searchParams.get("lastEventId");
  const lastEventId = Number(cursorRaw ?? 0);
  const resumingFrom = Number.isFinite(lastEventId) && lastEventId > 0;
  writeEvent(res, {
    type: "hello",
    sequence: resumingFrom ? lastEventId : await opts.log.latestSequence(),
    payload: { topic, status: "connected" },
  });
  if (cursorRaw != null && resumingFrom) {
    const replay = await opts.log.replay(topic, lastEventId, opts.replayLimit ?? 100);
    for (const event of replay) writeEvent(res, event);
  }

  let closed = false;
  const release = topic != null ? opts.clients?.open(topic) ?? null : null;
  const unsubscribe = opts.log.subscribe((event) => {
    if (closed) return;
    // A null-topic event is a broadcast: it goes to everyone. Otherwise a
    // topic-scoped client only sees its own topic.
    if (topic != null && event.topic != null && event.topic !== topic) return;
    writeEvent(res, event);
  });

  const heartbeatMs = opts.heartbeatMs ?? 15_000;
  const heartbeat = setInterval(() => {
    const write = (extra: unknown) => {
      // A heartbeatData hook resolves asynchronously, so the connection may have
      // closed in the meantime; never write to an ended response.
      if (closed || res.writableEnded) return;
      const base = { t: Date.now() };
      const data = extra && typeof extra === "object" ? { ...base, ...(extra as Record<string, unknown>) } : { ...base, data: extra };
      // No id line: an id on a heartbeat would overwrite the browser's
      // Last-Event-ID cursor with a heartbeat timestamp, breaking replay on the
      // next reconnect. Heartbeats carry liveness only, never a resume point.
      res.write(`event: heartbeat\ndata: ${JSON.stringify(data)}\n\n`);
    };
    if (opts.heartbeatData) {
      Promise.resolve(opts.heartbeatData(topic)).then(write).catch(() => write(undefined));
    } else {
      write(undefined);
    }
  }, heartbeatMs);
  heartbeat.unref();

  req.on("close", () => {
    closed = true;
    clearInterval(heartbeat);
    unsubscribe();
    release?.();
  });
  return true;
}

function writeEvent(res: ServerResponse, event: Pick<ReplayEvent, "type" | "sequence" | "payload">): void {
  res.write(`id: ${event.sequence}\n`);
  res.write(`event: ${event.type}\n`);
  res.write(`data: ${JSON.stringify(event.payload)}\n\n`);
}

function firstHeader(value: string | string[] | undefined): string | null {
  if (value == null) return null;
  return Array.isArray(value) ? (value[0] ?? null) : value;
}

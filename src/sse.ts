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
  /** Max events replayed on reconnect (default 100). A longer backlog yields a gap event. */
  replayLimit?: number;
  /** Heartbeat interval in ms (default 15000). */
  heartbeatMs?: number;
  /** Optional hook whose result is merged into each heartbeat frame (e.g. a
   *  freshness signal read from your own store). Receives the stream's topic. */
  heartbeatData?: (topic: string | null) => unknown | Promise<unknown>;
  /** Reconnect delay hint sent as the SSE `retry:` field. Omit for the browser default (~3s). */
  retryMs?: number;
  /** Event type naming a truncated backlog (default "gap"). */
  gapEventType?: string;
  /** Disconnect a client whose unflushed write buffer exceeds this many bytes
   *  (default 1 MiB). Set 0 to never disconnect. */
  maxBufferedBytes?: number;
}

/** Payload of the gap event: the events this client will never receive. */
export interface GapPayload {
  /** The cursor the client resumed from. */
  resumedFrom: number;
  /** First and last sequence in the lost range (inclusive). */
  missedFrom: number;
  missedThrough: number;
  /** How many visible events fall in that range. */
  missed: number;
  /** The replayLimit that caused the truncation. */
  replayLimit: number;
}

const DEFAULT_MAX_BUFFERED_BYTES = 1024 * 1024;

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
  const replayLimit = Math.max(0, opts.replayLimit ?? 100);
  const maxBufferedBytes = opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;

  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
  });
  if (opts.retryMs != null && Number.isFinite(opts.retryMs) && opts.retryMs >= 0) {
    // A lone retry field sets the browser's reconnect delay without dispatching
    // an event, so it costs nothing to send before the stream proper begins.
    res.write(`retry: ${Math.floor(opts.retryMs)}\n\n`);
  }

  const cursorRaw = firstHeader(req.headers["last-event-id"]) ?? url.searchParams.get("lastEventId");
  const lastEventId = Number(cursorRaw ?? 0);
  const resumingFrom = cursorRaw != null && Number.isFinite(lastEventId) && lastEventId > 0;

  let closed = false;
  let live = false;
  // Highest sequence already written to this response. Replay and the buffered
  // live events overlap by construction, so every write goes through this guard.
  let lastSent = resumingFrom ? lastEventId : 0;
  let pending: ReplayEvent[] = [];
  let heartbeat: ReturnType<typeof setInterval> | null = null;

  const visible = (event: ReplayEvent) => topic == null || event.topic == null || event.topic === topic;

  const emit = (event: ReplayEvent): void => {
    if (closed || res.writableEnded) return;
    if (event.sequence <= lastSent) return;
    lastSent = event.sequence;
    writeFrame(res, event.type, event.payload, event.sequence);
    // A consumer that reads slower than we publish would otherwise buffer the
    // whole backlog in this process's memory. Dropping the connection is safe
    // precisely because the log is durable: the client reconnects and resumes
    // from its Last-Event-ID (with a gap event if it fell far enough behind).
    if (maxBufferedBytes > 0 && res.writableLength > maxBufferedBytes) {
      cleanup();
      res.end();
    }
  };

  // Subscribe BEFORE reading the head or replaying. Anything appended during the
  // awaits below lands in `pending` instead of falling into the window between
  // the replay query and the live subscription, which would drop it silently.
  const unsubscribe = opts.log.subscribe((event) => {
    if (closed || !visible(event)) return;
    if (live) emit(event);
    else pending.push(event);
  });
  const release = topic != null ? opts.clients?.open(topic) ?? null : null;

  function cleanup(): void {
    if (closed) return;
    closed = true;
    if (heartbeat) clearInterval(heartbeat);
    unsubscribe();
    release?.();
  }

  req.on("close", cleanup);

  try {
    writeFrame(res, "hello", { topic, status: "connected" }, resumingFrom ? lastEventId : await opts.log.latestSequence());
    if (resumingFrom) {
      // One extra row is all it takes to tell "exactly the backlog" from "more
      // than we will ever send".
      const window = await opts.log.replay(topic, lastEventId, replayLimit + 1);
      if (window.length > replayLimit) {
        const recent = await opts.log.replayLatest(topic, lastEventId, replayLimit);
        const missedThrough = recent[0].sequence - 1;
        // Counting the closed range (lastEventId, recent[0].sequence) is exact
        // and race-free: appends only extend the newest end of the log.
        const missed = await opts.log.countBetween(topic, lastEventId, recent[0].sequence);
        // No id line. The gap frame is a notice, not a resume point: adopting it
        // as the cursor would let the client skip the range it was just told to
        // re-sync. Sent before the events so the client knows its state is
        // incomplete before it starts applying deltas on top of it.
        writeFrame(res, opts.gapEventType ?? "gap", {
          resumedFrom: lastEventId,
          missedFrom: lastEventId + 1,
          missedThrough,
          missed,
          replayLimit,
        } satisfies GapPayload, null);
        for (const event of recent) emit(event);
      } else {
        for (const event of window) emit(event);
      }
    }
  } catch {
    // A failed handshake (usually the database) must not leak the subscription
    // or the client-tracker slot, and must not leave the response hanging.
    cleanup();
    if (!res.writableEnded) res.end();
    return true;
  }

  if (closed) return true;

  // Everything buffered during the handshake, minus whatever replay already
  // wrote, then straight through from here on.
  live = true;
  for (const event of pending) emit(event);
  pending = [];
  if (closed) return true;

  const heartbeatMs = opts.heartbeatMs ?? 15_000;
  heartbeat = setInterval(() => {
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

  return true;
}

function writeFrame(res: ServerResponse, type: string, payload: unknown, sequence: number | null): void {
  // A null sequence deliberately omits the id line: the frame carries no resume
  // point (hello for a fresh client is the exception, it reports the head).
  if (sequence != null) res.write(`id: ${sequence}\n`);
  res.write(`event: ${type}\n`);
  // stringify returns undefined for a top-level undefined; never interpolate it.
  res.write(`data: ${JSON.stringify(payload) ?? "null"}\n\n`);
}

function firstHeader(value: string | string[] | undefined): string | null {
  if (value == null) return null;
  return Array.isArray(value) ? (value[0] ?? null) : value;
}

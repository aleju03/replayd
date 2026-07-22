import type { IncomingMessage, ServerResponse } from "node:http";
import type { Db } from "./db.js";
import { EventLog } from "./event-log.js";
import type { EventSink, ReplayEvent } from "./event-log.js";
import { TopicClientTracker } from "./topic-clients.js";
import type { TopicClientStats } from "./topic-clients.js";
import { handleSse } from "./sse.js";
import type { HandleSseOptions } from "./sse.js";

// The one-object entry point: a durable EventLog plus a TopicClientTracker,
// with an ergonomic publish() and a handleSse() already bound to both. Construct
// it with a Db, call ensureSchema() once, then publish() to append-and-fan-out
// and mount handleSse() on your server. Reach through to .log / .clients for the
// lower-level pieces when you need them.

export interface EventHubOptions {
  /** Default event type used by publish() when none is given (default "message"). */
  defaultType?: string;
  /** SSE options (path, topicParam, allowedOrigins, replayLimit, heartbeat*).
   *  log and clients are supplied by the hub. */
  sse?: Omit<HandleSseOptions, "log" | "clients">;
}

export interface PublishOptions {
  /** Event type; falls back to the hub's defaultType. */
  type?: string;
  /** Dedupe key; a repeated publish with the same id is a no-op. */
  eventId?: string;
}

export interface HubStats {
  latestSequence: number;
  topics: TopicClientStats[];
}

export class EventHub {
  readonly log: EventLog;
  readonly clients: TopicClientTracker;
  private readonly defaultType: string;
  private readonly sseOptions: Omit<HandleSseOptions, "log" | "clients">;

  constructor(db: Db, options: EventHubOptions = {}) {
    this.log = new EventLog(db);
    this.clients = new TopicClientTracker();
    this.defaultType = options.defaultType ?? "message";
    this.sseOptions = options.sse ?? {};
  }

  ensureSchema(): Promise<void> {
    return this.log.ensureSchema();
  }

  /** Append an event and fan it out. topic null is a broadcast to every subscriber. */
  publish(topic: string | null, payload: unknown, opts: PublishOptions = {}): Promise<ReplayEvent> {
    return this.log.append(opts.type ?? this.defaultType, topic, payload, opts.eventId);
  }

  subscribe(sink: EventSink): () => void {
    return this.log.subscribe(sink);
  }

  replay(topic: string | null, since: number, limit?: number): Promise<ReplayEvent[]> {
    return this.log.replay(topic, since, limit);
  }

  latestSequence(): Promise<number> {
    return this.log.latestSequence();
  }

  startTail(intervalMs: number): () => void {
    return this.log.startTail(intervalMs);
  }

  async stats(): Promise<HubStats> {
    return { latestSequence: await this.latestSequence(), topics: this.clients.snapshot() };
  }

  handleSse(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    return handleSse(req, res, { ...this.sseOptions, log: this.log, clients: this.clients });
  }
}

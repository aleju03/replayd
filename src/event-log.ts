import type { Db } from "./db.js";
import { exec, json, nowIso, parseJson } from "./db.js";

// The durable event log: an append-only, autoincrementing SQLite table that is
// both the replay backbone (a reconnecting client resumes from its last
// sequence) and a fan-out point for in-process live subscribers. It also
// supports a server/worker split: one process appends (the worker) while
// another polls the table with startTail() and forwards new rows to its own SSE
// sinks (the server), so a write on any connection reaches every reader.

export interface ReplayEvent {
  /** Monotonic autoincrement id; the SSE Last-Event-ID cursor. */
  sequence: number;
  /** Caller-supplied or generated dedupe key (unique). */
  event_id: string;
  /** Event type, surfaced as the SSE "event:" field. */
  type: string;
  /** null means a global/broadcast event, visible on every topic. */
  topic: string | null;
  payload: unknown;
  /** ISO-8601 creation time. */
  created_at: string;
}

export type EventSink = (event: ReplayEvent) => void;

const TAIL_BATCH_LIMIT = 200;

export class EventLog {
  private sinks = new Set<EventSink>();
  private tailing = false;

  constructor(private readonly db: Db) {}

  async ensureSchema(): Promise<void> {
    await exec(
      this.db,
      `create table if not exists live_event_log (
         sequence integer primary key autoincrement,
         event_id text not null unique,
         type text not null,
         topic text,
         payload_json text not null,
         created_at text not null
       )`,
    );
    await exec(
      this.db,
      `create index if not exists idx_live_event_topic_sequence on live_event_log(topic, sequence)`,
    );
  }

  subscribe(sink: EventSink): () => void {
    this.sinks.add(sink);
    return () => this.sinks.delete(sink);
  }

  private dispatch(event: ReplayEvent): void {
    for (const sink of this.sinks) sink(event);
  }

  // Used by a serving process that does not append itself: poll the log for
  // newly appended rows (written by the worker process) and fan them out to the
  // local SSE sinks. While tailing, append() stops dispatching directly so the
  // poller is the single delivery path (no double-send if this process appends).
  startTail(intervalMs: number): () => void {
    this.tailing = true;
    let stopped = false;
    let cursor = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const schedule = (delay: number) => {
      if (stopped) return;
      timer = setTimeout(tick, delay);
      timer.unref();
    };
    const tick = async () => {
      if (stopped) return;
      try {
        const batch = await this.replay(null, cursor, TAIL_BATCH_LIMIT);
        for (const event of batch) {
          if (event.sequence > cursor) cursor = event.sequence;
          this.dispatch(event);
        }
        // A full batch likely means more is waiting; drain without idling.
        schedule(batch.length >= TAIL_BATCH_LIMIT ? 0 : intervalMs);
      } catch {
        // A failed poll (usually a transient BUSY) must never kill the tail;
        // back off and try again.
        schedule(Math.max(intervalMs, 1_000));
      }
    };
    // Start from the current head so we only forward events created from now on;
    // SSE clients receive their own backlog via replay-on-connect. Retry head
    // detection on failure rather than tailing from sequence 0, which would
    // replay the entire log history into live sinks.
    const initCursor = async () => {
      if (stopped) return;
      try {
        cursor = await this.latestSequence();
        schedule(intervalMs);
      } catch {
        if (stopped) return;
        timer = setTimeout(initCursor, 1_000);
        timer.unref();
      }
    };
    void initCursor();
    return () => {
      stopped = true;
      this.tailing = false;
      if (timer) clearTimeout(timer);
    };
  }

  async append(type: string, topic: string | null, payload: unknown, eventId?: string): Promise<ReplayEvent> {
    const createdAt = nowIso();
    const stableId = eventId ?? `${type}:${topic ?? "global"}:${createdAt}:${Math.random().toString(36).slice(2)}`;
    // insert or ignore makes append idempotent by event_id: a retried publish
    // (same eventId) neither duplicates the row nor re-dispatches to live sinks.
    const result = await exec(
      this.db,
      `insert or ignore into live_event_log (event_id, type, topic, payload_json, created_at)
       values (?, ?, ?, ?, ?)`,
      [stableId, type, topic, json(payload), createdAt],
    );
    const row = (await exec(this.db, "select * from live_event_log where event_id = ?", [stableId])).rows[0];
    // The stored JSON is the payload verbatim. If you ever need to compact a
    // large payload into a reference and rehydrate it on read, do it in your
    // own publish/read wrappers; the log stays a plain append-only store.
    const event = rowToReplayEvent(row);
    if (Number(result.rowsAffected ?? 0) > 0 && !this.tailing) {
      this.dispatch(event);
    }
    return event;
  }

  async replay(topic: string | null, since: number, limit = 100): Promise<ReplayEvent[]> {
    // topic is null: subscriber wants every topic (the global aggregate).
    // topic set: rows for that topic plus null-topic (broadcast) rows.
    const result = await exec(
      this.db,
      `select * from live_event_log
       where sequence > ? and (topic is null or ? is null or topic = ?)
       order by sequence asc
       limit ?`,
      [since, topic, topic, limit],
    );
    return result.rows.map(rowToReplayEvent);
  }

  async latestSequence(): Promise<number> {
    const row = (await exec(this.db, "select coalesce(max(sequence), 0) as sequence from live_event_log")).rows[0];
    return Number(row?.sequence ?? 0);
  }
}

function rowToReplayEvent(row: Record<string, unknown>): ReplayEvent {
  return {
    sequence: Number(row.sequence),
    event_id: String(row.event_id),
    type: String(row.type),
    topic: row.topic == null ? null : String(row.topic),
    payload: parseJson(row.payload_json, null),
    created_at: String(row.created_at),
  };
}

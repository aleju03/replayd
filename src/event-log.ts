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

/** Bounds on how much history the log keeps. Both limits may be combined. */
export interface RetentionPolicy {
  /** Keep at most this many of the newest events. */
  maxEvents?: number;
  /** Drop events created longer ago than this. */
  maxAgeMs?: number;
}

export interface StartTailOptions {
  /** Idle ceiling for the poll interval. Omit to poll at a fixed intervalMs. */
  maxIntervalMs?: number;
}

const TAIL_BATCH_LIMIT = 200;
const DEFAULT_RETENTION_INTERVAL_MS = 60 * 60 * 1_000;

// Every read that serves a subscriber applies the same visibility rule, so it
// lives in one place: a subscriber with no topic (null) sees the global
// aggregate, and a topic-scoped subscriber sees its own topic plus null-topic
// broadcasts. Callers pass [topic, topic] for the two placeholders.
const TOPIC_PREDICATE = "(topic is null or ? is null or topic = ?)";

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
  startTail(intervalMs: number, options: StartTailOptions = {}): () => void {
    // Two concurrent tails on one log would dispatch every row to every sink
    // twice, and the first stop() would clear the shared tailing flag and let
    // append() start double-dispatching as well. It is always a wiring mistake.
    if (this.tailing) throw new Error("EventLog is already tailing; stop the existing tail before starting another");
    this.tailing = true;
    // An idle ceiling trades worst-case delivery latency for far fewer wasted
    // queries on a quiet log. Without maxIntervalMs the cadence is unchanged.
    const maxIntervalMs = Math.max(intervalMs, options.maxIntervalMs ?? intervalMs);
    let stopped = false;
    let cursor = 0;
    let idleDelay = intervalMs;
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
        if (batch.length > 0) {
          idleDelay = intervalMs;
          // A full batch likely means more is waiting; drain without idling.
          schedule(batch.length >= TAIL_BATCH_LIMIT ? 0 : intervalMs);
        } else {
          schedule(idleDelay);
          idleDelay = Math.min(maxIntervalMs, Math.ceil(idleDelay * 1.5));
        }
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
      if (stopped) return;
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

  /** The oldest events after `since` that this subscriber can see, in order. */
  async replay(topic: string | null, since: number, limit = 100): Promise<ReplayEvent[]> {
    const result = await exec(
      this.db,
      `select * from live_event_log
       where sequence > ? and ${TOPIC_PREDICATE}
       order by sequence asc
       limit ?`,
      [since, topic, topic, limit],
    );
    return result.rows.map(rowToReplayEvent);
  }

  // The newest events after `since` rather than the oldest, still returned in
  // ascending order. A client whose backlog outgrew the replay limit is better
  // served the current tail plus an explicit gap notice than the stale head of a
  // backlog it can never finish draining.
  async replayLatest(topic: string | null, since: number, limit = 100): Promise<ReplayEvent[]> {
    const result = await exec(
      this.db,
      `select * from (
         select * from live_event_log
         where sequence > ? and ${TOPIC_PREDICATE}
         order by sequence desc
         limit ?
       ) order by sequence asc`,
      [since, topic, topic, limit],
    );
    return result.rows.map(rowToReplayEvent);
  }

  /** How many visible events fall in the exclusive range (after, before). */
  async countBetween(topic: string | null, after: number, before: number): Promise<number> {
    const row = (await exec(
      this.db,
      `select count(*) as n from live_event_log
       where sequence > ? and sequence < ? and ${TOPIC_PREDICATE}`,
      [after, before, topic, topic],
    )).rows[0];
    return Number(row?.n ?? 0);
  }

  async latestSequence(): Promise<number> {
    const row = (await exec(this.db, "select coalesce(max(sequence), 0) as sequence from live_event_log")).rows[0];
    return Number(row?.sequence ?? 0);
  }

  // Nothing trims the log on its own: an append-only table that is never pruned
  // grows without bound. Enforce a policy here (once, or on an interval via
  // startRetention) and note that pruning below a disconnected client's cursor
  // is what turns its next reconnect into a gap event rather than a full replay.
  async prune(policy: RetentionPolicy): Promise<number> {
    let deleted = 0;
    if (policy.maxEvents != null && policy.maxEvents >= 0) {
      // The offset row is the newest event that must NOT survive. With fewer
      // rows than maxEvents the subquery yields null, the comparison is null,
      // and nothing is deleted.
      const result = await exec(
        this.db,
        `delete from live_event_log
         where sequence <= (select sequence from live_event_log order by sequence desc limit 1 offset ?)`,
        [policy.maxEvents],
      );
      deleted += Number(result.rowsAffected ?? 0);
    }
    if (policy.maxAgeMs != null && policy.maxAgeMs >= 0) {
      // created_at is always a UTC ISO-8601 string from nowIso(), a format whose
      // lexicographic order matches chronological order, so a plain text
      // comparison is a correct time filter.
      const cutoff = new Date(Date.now() - policy.maxAgeMs).toISOString();
      const result = await exec(this.db, `delete from live_event_log where created_at < ?`, [cutoff]);
      deleted += Number(result.rowsAffected ?? 0);
    }
    return deleted;
  }

  /** Run prune(policy) on an interval. Returns a stop function. */
  startRetention(policy: RetentionPolicy, intervalMs = DEFAULT_RETENTION_INTERVAL_MS): () => void {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const schedule = () => {
      if (stopped) return;
      timer = setTimeout(tick, intervalMs);
      timer.unref();
    };
    const tick = async () => {
      if (stopped) return;
      // A failed prune is not fatal: the log keeps growing until the next pass.
      await this.prune(policy).catch(() => 0);
      schedule();
    };
    schedule();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
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

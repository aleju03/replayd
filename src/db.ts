import { createClient, type Client, type InValue, type ResultSet, type TransactionMode } from "@libsql/client";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

// A thin wrapper over @libsql/client that adds the three things a real
// SQLite deployment needs and the raw client leaves to you: the
// SQLITE_BUSY / SQLITE_LOCKED retry loop (a WAL writer plus concurrent
// readers WILL collide), the WAL-friendly connection pragmas, and small
// json helpers. Local libsql runs every statement synchronously on the
// calling thread; the async surface here is also what lets a remote
// (Turso) URL drop in unchanged.

export type Db = Client;

export interface DbStatement {
  sql: string;
  args?: InValue[];
}

export interface CreateDbOptions {
  /** libsql URL: "file:events.db", ":memory:", or a remote libsql/Turso URL. */
  url: string;
  authToken?: string;
  /** pragma busy_timeout (default 5000ms). */
  busyTimeoutMs?: number;
  /** pragma synchronous (default NORMAL, which is durable under WAL). */
  synchronous?: "OFF" | "NORMAL" | "FULL" | "EXTRA";
  /** JS-side retry budget on BUSY/LOCKED before the error propagates (default 15000ms). */
  busyRetryMs?: number;
}

const DEFAULT_BUSY_RETRY_MS = 15_000;
const BUSY_RETRY_INITIAL_DELAY_MS = 25;
const BUSY_RETRY_MAX_DELAY_MS = 500;
const EXEC_BATCH_MAX_STATEMENTS = 500;

const busyRetryBudget = new WeakMap<Db, number>();

export async function createDb(options: CreateDbOptions): Promise<Db> {
  const isFile = options.url.startsWith("file:");
  if (isFile) {
    const filePath = options.url.slice("file:".length);
    if (filePath && filePath !== ":memory:") {
      await mkdir(dirname(resolve(filePath)), { recursive: true });
    }
  }
  const db = createClient({ url: options.url, authToken: options.authToken });
  busyRetryBudget.set(db, options.busyRetryMs ?? DEFAULT_BUSY_RETRY_MS);
  if (isFile) await applyPragmas(db, options);
  return db;
}

async function applyPragmas(db: Db, options: CreateDbOptions): Promise<void> {
  // Local libsql runs every query on the calling thread and each process opens
  // its own connection, so connection-level pragmas must be set here. Remote
  // URLs manage these server-side, so they are skipped by createDb above.
  const busyTimeoutMs = options.busyTimeoutMs ?? 5_000;
  const synchronous = (options.synchronous ?? "NORMAL").toUpperCase();
  const pragmas = [
    `pragma journal_mode = WAL`,
    // busy_timeout makes a connection wait for a lock instead of failing with
    // SQLITE_BUSY immediately; mandatory once a second connection writes the
    // same WAL file concurrently.
    `pragma busy_timeout = ${busyTimeoutMs}`,
    // NORMAL fsyncs only at checkpoints (not every commit) and stays durable
    // under WAL: a crash can lose the last few committed transactions but never
    // corrupts the database.
    `pragma synchronous = ${/^(OFF|NORMAL|FULL|EXTRA)$/.test(synchronous) ? synchronous : "NORMAL"}`,
    `pragma wal_autocheckpoint = 1000`,
    // The WAL file never shrinks on its own; cap it so a past write burst does
    // not leave a multi-hundred-MB file slowing every restart's WAL recovery.
    `pragma journal_size_limit = ${64 * 1024 * 1024}`,
  ];
  for (const pragma of pragmas) {
    await db.execute(pragma).catch(() => undefined);
  }
}

export async function exec(db: Db, sql: string, args: InValue[] = []): Promise<ResultSet> {
  return withBusyRetry(db, () => db.execute({ sql, args }));
}

export async function execBatch(db: Db, statements: DbStatement[], mode: TransactionMode = "write"): Promise<ResultSet[]> {
  if (statements.length === 0) return [];
  const results: ResultSet[] = [];
  for (let i = 0; i < statements.length; i += EXEC_BATCH_MAX_STATEMENTS) {
    const chunk = statements.slice(i, i + EXEC_BATCH_MAX_STATEMENTS);
    results.push(...await withBusyRetry(db, () => db.batch(chunk.map(({ sql, args = [] }) => ({ sql, args })), mode)));
  }
  return results;
}

export function isSqliteBusyError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /SQLITE_(BUSY|LOCKED)|database is locked|database table is locked/i.test(message);
}

async function withBusyRetry<T>(db: Db, operation: () => Promise<T>): Promise<T> {
  const budgetMs = busyRetryBudget.get(db) ?? DEFAULT_BUSY_RETRY_MS;
  if (budgetMs <= 0) return operation();
  const startedAt = Date.now();
  let delayMs = BUSY_RETRY_INITIAL_DELAY_MS;
  for (;;) {
    try {
      return await operation();
    } catch (error) {
      if (!isSqliteBusyError(error)) throw error;
      const elapsed = Date.now() - startedAt;
      if (elapsed >= budgetMs) throw error;
      const waitMs = Math.min(delayMs, budgetMs - elapsed);
      await sleep(waitMs);
      delayMs = Math.min(BUSY_RETRY_MAX_DELAY_MS, Math.ceil(delayMs * 1.6));
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((done) => setTimeout(done, Math.max(1, Math.ceil(ms))));
}

export function json<T>(value: T): string {
  // JSON.stringify returns undefined for a top-level undefined, function, or
  // symbol; coerce to "null" so this always yields a string and never feeds
  // undefined into a NOT NULL column (which libsql rejects).
  const serialized = JSON.stringify(value);
  return serialized === undefined ? "null" : serialized;
}

export function parseJson<T>(value: unknown, fallback: T): T {
  if (typeof value !== "string") return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

export function nowMs(): number {
  return Date.now();
}

export function nowIso(): string {
  return new Date().toISOString();
}

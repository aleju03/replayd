import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createDb, EventLog, exec, type ReplayEvent } from "../src/index.js";

const dirs: string[] = [];

afterEach(async () => {
  await Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  dirs.length = 0;
});

// The tail tests need TWO connections to the SAME file DB (one appends, one
// tails), so they use temp file dbs, not :memory: (which is per-connection).
async function tempDbUrl(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "replayd-eventlog-"));
  dirs.push(dir);
  return `file:${join(dir, "test.db")}`;
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 2_000): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe("event log", () => {
  it("append stores and returns the payload", async () => {
    const db = await createDb({ url: ":memory:" });
    const log = new EventLog(db);
    await log.ensureSchema();

    const payload = { hello: "world", nested: { n: 42 }, list: [1, 2, 3] };
    const event = await log.append("snipe", "CR", payload, "evt-store");

    expect(event.event_id).toBe("evt-store");
    expect(event.type).toBe("snipe");
    expect(event.topic).toBe("CR");
    expect(event.sequence).toBeGreaterThan(0);
    expect(event.payload).toEqual(payload);
    expect(typeof event.created_at).toBe("string");

    // Persisted verbatim: a fresh read via replay returns the same payload.
    const [replayed] = await log.replay("CR", 0, 10);
    expect(replayed.payload).toEqual(payload);
    expect(replayed.event_id).toBe("evt-store");
    db.close();
  });

  it("dedupes a repeated append by event_id", async () => {
    const db = await createDb({ url: ":memory:" });
    const log = new EventLog(db);
    await log.ensureSchema();

    const first = await log.append("snipe", "CR", { n: 1 }, "same-id");
    const second = await log.append("snipe", "CR", { n: 2 }, "same-id");
    expect(second.sequence).toBe(first.sequence);
    const all = await log.replay(null, 0, 10);
    expect(all).toHaveLength(1);
    expect(all[0].payload).toEqual({ n: 1 });
    db.close();
  });
});

describe("event log tail (server/worker split)", () => {
  it("delivers events appended on a separate worker connection to a tailing server connection", async () => {
    const url = await tempDbUrl();
    const writerDb = await createDb({ url });
    const writer = new EventLog(writerDb); // worker process: appends, no tailing
    await writer.ensureSchema();
    const readerDb = await createDb({ url });
    const reader = new EventLog(readerDb); // server process: tails for SSE

    const received: ReplayEvent[] = [];
    reader.subscribe((event) => received.push(event));
    const stop = reader.startTail(25);
    // Let the tailer pin its cursor at the current head before we append.
    await new Promise((resolve) => setTimeout(resolve, 100));

    await writer.append("snipe", "CR", { hello: "world" }, "evt-1");

    await waitFor(() => received.some((event) => event.event_id === "evt-1"));
    // Exactly one delivery: the cross-connection write is forwarded once.
    expect(received.filter((event) => event.event_id === "evt-1")).toHaveLength(1);
    expect(received.find((event) => event.event_id === "evt-1")?.payload).toMatchObject({ hello: "world" });
    expect(received.find((event) => event.event_id === "evt-1")?.topic).toBe("CR");

    stop();
    writerDb.close();
    readerDb.close();
  });

  it("does not double-deliver when the tailing connection also appends", async () => {
    const url = await tempDbUrl();
    const db = await createDb({ url });
    const log = new EventLog(db);
    await log.ensureSchema();

    const received: ReplayEvent[] = [];
    log.subscribe((event) => received.push(event));
    const stop = log.startTail(25);
    await new Promise((resolve) => setTimeout(resolve, 100));

    // While tailing, append() must skip its in-process dispatch so the poller is
    // the single delivery path (otherwise events would be sent twice).
    await log.append("snipe", "CR", { n: 1 }, "evt-x");

    await waitFor(() => received.some((event) => event.event_id === "evt-x"));
    await new Promise((resolve) => setTimeout(resolve, 75)); // allow a second poll to (not) re-send
    expect(received.filter((event) => event.event_id === "evt-x")).toHaveLength(1);

    stop();
    db.close();
  });

  it("forwards only events created after the tail starts", async () => {
    const url = await tempDbUrl();
    const writerDb = await createDb({ url });
    const writer = new EventLog(writerDb);
    await writer.ensureSchema();
    const readerDb = await createDb({ url });
    const reader = new EventLog(readerDb);

    // Pre-existing history must not be replayed to live sinks (SSE clients get
    // their own backlog via replay-on-connect, not the tailer).
    await writer.append("snipe", "CR", { old: true }, "evt-old");

    const received: ReplayEvent[] = [];
    reader.subscribe((event) => received.push(event));
    const stop = reader.startTail(25);
    await new Promise((resolve) => setTimeout(resolve, 100));

    await writer.append("snipe", "CR", { fresh: true }, "evt-new");
    await waitFor(() => received.some((event) => event.event_id === "evt-new"));

    expect(received.some((event) => event.event_id === "evt-old")).toBe(false);
    expect(received.some((event) => event.event_id === "evt-new")).toBe(true);

    stop();
    writerDb.close();
    readerDb.close();
  });

  it("refuses a second concurrent tail on the same log", async () => {
    const url = await tempDbUrl();
    const db = await createDb({ url });
    const log = new EventLog(db);
    await log.ensureSchema();

    // Two pollers on one log would dispatch every row to every sink twice.
    const stop = log.startTail(25);
    expect(() => log.startTail(25)).toThrow(/already tailing/);

    // Stopping releases the claim, so a later tail is allowed.
    stop();
    log.startTail(25)();
    db.close();
  });
});

describe("retention", () => {
  it("prunes down to the newest maxEvents", async () => {
    const db = await createDb({ url: ":memory:" });
    const log = new EventLog(db);
    await log.ensureSchema();
    for (let n = 1; n <= 5; n += 1) await log.append("snipe", "CR", { n }, `evt-${n}`);

    expect(await log.prune({ maxEvents: 2 })).toBe(3);
    const remaining = await log.replay(null, 0, 100);
    expect(remaining.map((event) => (event.payload as { n: number }).n)).toEqual([4, 5]);

    // Already within policy: a second pass deletes nothing.
    expect(await log.prune({ maxEvents: 2 })).toBe(0);
    // And a policy larger than the log is a no-op, not an error.
    expect(await log.prune({ maxEvents: 50 })).toBe(0);
    db.close();
  });

  it("prunes events older than maxAgeMs", async () => {
    const db = await createDb({ url: ":memory:" });
    const log = new EventLog(db);
    await log.ensureSchema();
    await log.append("snipe", "CR", { n: 1 }, "evt-old");
    await log.append("snipe", "CR", { n: 2 }, "evt-new");

    // Backdate one row rather than sleeping for the age window to pass.
    await exec(db, "update live_event_log set created_at = ? where event_id = ?", [
      new Date(Date.now() - 60_000).toISOString(),
      "evt-old",
    ]);

    expect(await log.prune({ maxAgeMs: 30_000 })).toBe(1);
    const remaining = await log.replay(null, 0, 100);
    expect(remaining.map((event) => event.event_id)).toEqual(["evt-new"]);
    db.close();
  });

  it("startRetention enforces the policy on an interval", async () => {
    const db = await createDb({ url: ":memory:" });
    const log = new EventLog(db);
    await log.ensureSchema();
    for (let n = 1; n <= 4; n += 1) await log.append("snipe", "CR", { n }, `evt-${n}`);

    const stop = log.startRetention({ maxEvents: 1 }, 20);
    await waitFor(async () => (await log.replay(null, 0, 100)).length === 1);
    stop();

    const remaining = await log.replay(null, 0, 100);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].event_id).toBe("evt-4");
    db.close();
  });
});

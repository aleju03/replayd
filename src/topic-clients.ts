import { nowIso } from "./db.js";

// An in-memory, per-topic connection counter. handleSse calls open(topic) when
// a client subscribes and invokes the returned release when it disconnects, so
// snapshot() answers "how many clients are watching each topic right now". It is
// intentionally per-process (each serving instance tracks its own connections)
// and is not part of the durable log.

export interface TopicClientStats {
  topic: string;
  activeClients: number;
  lastActiveAt: string | null;
}

const MAX_TOPIC_LENGTH = 128;

export class TopicClientTracker {
  private readonly topics = new Map<string, TopicClientStats>();

  open(topic: string): () => void {
    const normalized = normalizeTopic(topic);
    const current = this.topics.get(normalized) ?? { topic: normalized, activeClients: 0, lastActiveAt: null };
    current.activeClients += 1;
    current.lastActiveAt = nowIso();
    this.topics.set(normalized, current);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.topics.get(normalized);
      if (!next) return;
      next.activeClients = Math.max(0, next.activeClients - 1);
      next.lastActiveAt = nowIso();
      this.topics.set(normalized, next);
    };
  }

  snapshot(): TopicClientStats[] {
    return [...this.topics.values()].map((entry) => ({ ...entry }));
  }
}

// A topic is an arbitrary string, so just trim and bound its length. Do not
// uppercase or otherwise canonicalize it: topics are opaque identifiers.
function normalizeTopic(topic: string): string {
  return topic.trim().slice(0, MAX_TOPIC_LENGTH);
}

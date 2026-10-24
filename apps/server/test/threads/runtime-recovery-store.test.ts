import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createMigratedConnection } from "@bb/db/testing";
import { createConnection } from "@bb/db";
import {
  readRecovery,
  readRecoveryEpisode,
  writeRecovery,
  readRecoveryHost,
  writeRecoveryHost,
  reserveRecovery,
  cancelRuntimeRecovery,
  reconcileRecoveryHost,
  connectRecoveryHost,
  type RecoveryState,
} from "../../src/services/threads/runtime-recovery-store.js";
const now = 1_790_000_000_000;
function state(): RecoveryState {
  return {
    mode: "read-only",
    generation: 1,
    originalRequestId: "original",
    expectedRequestId: "original",
    hostId: "host",
    openedAt: now - 120_000,
    notBefore: now,
    attempts: [],
    decision: "test",
  };
}
function setup() {
  const db = createMigratedConnection();
  writeRecovery(db, "thread", state());
  reconcileRecoveryHost(db, "host", "session", now - 60_000);
  connectRecoveryHost(db, "host", "session", now - 60_000);
  return db;
}
describe("durable recovery admission", () => {
  it("is off for every thread without an explicit grant", () => {
    const db = createMigratedConnection();
    try {
      expect(readRecovery(db, "thread")).toBeNull();
      expect(() => reserveRecovery(db, "thread", 0, now)).toThrow();
    } finally {
      db.$client.close();
    }
  });
  it("one failed request earns one reservation even across restart", () => {
    const db = setup();
    db.transaction((tx) => reserveRecovery(tx, "thread", 1, now));
    const restarted = createConnection(db.$client.serialize());
    try {
      expect(readRecovery(restarted, "thread")!.attempts).toHaveLength(1);
      expect(() => reserveRecovery(restarted, "thread", 2, now)).toThrow(
        "recovery_already_attempted",
      );
    } finally {
      db.$client.close();
      restarted.$client.close();
    }
  });
  it("fences the second worker with its old generation", () => {
    const db = setup();
    try {
      reserveRecovery(db, "thread", 1, now);
      expect(() => reserveRecovery(db, "thread", 1, now)).toThrow(
        "recovery_generation_changed",
      );
    } finally {
      db.$client.close();
    }
  });
  it("fences two independent SQLite connections and preserves their claim after reopen", () => {
    const template = setup();
    const directory = mkdtempSync(join(tmpdir(), "bb-recovery-workers-"));
    const path = join(directory, "state.db");
    writeFileSync(path, template.$client.serialize());
    template.$client.close();
    const first = createConnection(path),
      second = createConnection(path);
    try {
      first.transaction((tx) => reserveRecovery(tx, "thread", 1, now), {
        behavior: "immediate",
      });
      expect(() =>
        second.transaction((tx) => reserveRecovery(tx, "thread", 1, now), {
          behavior: "immediate",
        }),
      ).toThrow("recovery_generation_changed");
      expect(readRecovery(second, "thread")!.attempts).toHaveLength(1);
    } finally {
      first.$client.close();
      second.$client.close();
    }
    const reopened = createConnection(path);
    try {
      expect(readRecovery(reopened, "thread")!.attempts).toHaveLength(1);
    } finally {
      reopened.$client.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });
  it("keeps the two-attempt budget and refuses a third request", () => {
    const db = setup();
    try {
      const first = reserveRecovery(db, "thread", 1, now)!;
      writeRecovery(db, "thread", {
        ...first,
        attempts: first.attempts.map((a) => ({
          ...a,
          phase: "dispatched" as const,
        })),
        expectedRequestId: "retry-1",
        notBefore: now + 180_000,
      });
      expect(() => reserveRecovery(db, "thread", 2, now + 179_999)).toThrow(
        "recovery_budget_or_delay",
      );
      const second = reserveRecovery(db, "thread", 2, now + 180_000)!;
      writeRecovery(db, "thread", { ...second, expectedRequestId: "retry-2" });
      expect(() => reserveRecovery(db, "thread", 3, now + 181_000)).toThrow(
        "recovery_budget_or_delay",
      );
    } finally {
      db.$client.close();
    }
  });
  it.each([59_999, 0, -1])("waits for host stability (%s ms)", (elapsed) => {
    const db = setup();
    try {
      reconcileRecoveryHost(db, "host", "new-session", now - elapsed);
      expect(() => reserveRecovery(db, "thread", 1, now)).toThrow(
        "host_not_stable",
      );
    } finally {
      db.$client.close();
    }
  });
  it("keeps only one recovery owner per host", () => {
    const db = setup();
    try {
      reserveRecovery(db, "thread", 1, now);
      writeRecovery(db, "other", state());
      expect(reserveRecovery(db, "other", 1, now)).toBeNull();
    } finally {
      db.$client.close();
    }
  });
  it("does not reset the episode after thirty minutes", () => {
    const db = setup();
    try {
      expect(() => reserveRecovery(db, "thread", 1, now + 1800_000)).toThrow(
        "recovery_budget_or_delay",
      );
    } finally {
      db.$client.close();
    }
  });
  it("cancellation survives restart and invalidates a pending dispatch", () => {
    const db = setup();
    try {
      reserveRecovery(db, "thread", 1, now);
      cancelRuntimeRecovery(db, "thread", "manual-stop");
      expect(readRecovery(db, "thread")!.generation).toBe(3);
      expect(() => reserveRecovery(db, "thread", 2, now)).toThrow(
        "recovery_generation_changed",
      );
    } finally {
      db.$client.close();
    }
  });
  it.each(["reserved", "queued", "dispatching", "unknown"] as const)(
    "never repeats an uncertain %s outcome after restart",
    (phase) => {
      const db = setup();
      try {
        const first = reserveRecovery(db, "thread", 1, now)!;
        writeRecovery(db, "thread", {
          ...first,
          expectedRequestId: "retry-1",
          attempts: first.attempts.map((a) => ({ ...a, phase })),
        });
        const restarted = createConnection(db.$client.serialize());
        try {
          expect(() =>
            reserveRecovery(restarted, "thread", 2, now + 180_000),
          ).toThrow("recovery_inflight_or_unknown");
        } finally {
          restarted.$client.close();
        }
      } finally {
        db.$client.close();
      }
    },
  );
  it("requires sixty seconds after the actual reconnect, even if reconcile was earlier", () => {
    const db = setup();
    try {
      connectRecoveryHost(db, "host", "session", now - 59_999);
      expect(() => reserveRecovery(db, "thread", 1, now)).toThrow(
        "host_not_stable",
      );
    } finally {
      db.$client.close();
    }
  });
  it("opens and durably preserves the host circuit on three distinct incidents", () => {
    const db = setup();
    try {
      for (let i = 0; i < 3; i++) {
        const id = `thread-${i}`;
        writeRecovery(db, id, state());
        const h = readRecoveryHost(db, "host");
        writeRecoveryHost(db, "host", { ...h, owner: null });
        db.transaction((tx) => reserveRecovery(tx, id, 1, now));
      }
      expect(readRecoveryHost(db, "host").blockedUntil).toBe(now + 600_000);
      expect(readRecovery(db, "thread-2")!.attempts).toHaveLength(0);
    } finally {
      db.$client.close();
    }
  });
  it("keeps the original request's dedup and budget when the thread moves to another task", () => {
    const db = setup();
    try {
      reserveRecovery(db, "thread", 1, now);
      cancelRuntimeRecovery(db, "thread", "new-input");
      writeRecovery(db, "thread", {
        ...state(),
        originalRequestId: "new-task",
        expectedRequestId: "new-task",
        generation: 4,
      });
      const old = readRecoveryEpisode(db, "thread", "original")!;
      expect(old.mode).toBe("off");
      expect(old.attempts).toHaveLength(1);
      const restarted = createConnection(db.$client.serialize());
      try {
        expect(
          readRecoveryEpisode(restarted, "thread", "original")!.attempts,
        ).toHaveLength(1);
      } finally {
        restarted.$client.close();
      }
    } finally {
      db.$client.close();
    }
  });
});

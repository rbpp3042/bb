import { and, eq, like, sql } from "drizzle-orm";
import { pluginKv, type DbQueryConnection } from "@bb/db";
import { z } from "zod";

const namespace = "core-runtime-recovery";
const attemptSchema = z.object({
  requestId: z.string(),
  generation: z.number(),
  at: z.number(),
  phase: z.enum(["reserved", "queued", "dispatching", "dispatched", "unknown"]),
  queueId: z.string().nullable(),
});
export const recoveryStateSchema = z.object({
  mode: z.enum(["off", "read-only"]),
  generation: z.number().int(),
  originalRequestId: z.string(),
  expectedRequestId: z.string(),
  hostId: z.string(),
  openedAt: z.number(),
  notBefore: z.number(),
  attempts: z.array(attemptSchema).max(2),
  decision: z.string(),
});
export type RecoveryState = z.infer<typeof recoveryStateSchema>;
const hostSchema = z.object({
  sessionId: z.string().nullable(),
  reconciledAt: z.number(),
  connectedAt: z.number(),
  blockedUntil: z.number(),
  incidents: z.array(z.object({ threadId: z.string(), at: z.number() })),
  owner: z.string().nullable(),
});
export type RecoveryHost = z.infer<typeof hostSchema>;

function read(db: DbQueryConnection, key: string): unknown {
  const row = db
    .select()
    .from(pluginKv)
    .where(and(eq(pluginKv.pluginId, namespace), eq(pluginKv.key, key)))
    .get();
  return row ? JSON.parse(row.value) : null;
}
function write(db: DbQueryConnection, key: string, value: unknown): void {
  db.insert(pluginKv)
    .values({
      pluginId: namespace,
      key,
      value: JSON.stringify(value),
      updatedAt: Date.now(),
    })
    .onConflictDoUpdate({
      target: [pluginKv.pluginId, pluginKv.key],
      set: { value: JSON.stringify(value), updatedAt: Date.now() },
    })
    .run();
}
export function readRecovery(
  db: DbQueryConnection,
  threadId: string,
): RecoveryState | null {
  return recoveryStateSchema.nullable().parse(read(db, `thread:${threadId}`));
}
export function readRecoveryEpisode(
  db: DbQueryConnection,
  threadId: string,
  originalRequestId: string,
): RecoveryState | null {
  return recoveryStateSchema
    .nullable()
    .parse(read(db, `episode:${threadId}:${originalRequestId}`));
}
export function writeRecovery(
  db: DbQueryConnection,
  threadId: string,
  state: RecoveryState,
): void {
  db.transaction(
    (tx) => {
      const parsed = recoveryStateSchema.parse(state);
      write(tx, `thread:${threadId}`, parsed);
      write(tx, `episode:${threadId}:${parsed.originalRequestId}`, parsed);
    },
    { behavior: "immediate" },
  );
}
export function readRecoveryHost(
  db: DbQueryConnection,
  hostId: string,
): RecoveryHost {
  return hostSchema.parse(
    read(db, `host:${hostId}`) ?? {
      sessionId: null,
      reconciledAt: 0,
      connectedAt: 0,
      blockedUntil: 0,
      incidents: [],
      owner: null,
    },
  );
}
export function writeRecoveryHost(
  db: DbQueryConnection,
  hostId: string,
  state: RecoveryHost,
): void {
  write(db, `host:${hostId}`, hostSchema.parse(state));
}
export function listRecoveryThreads(db: DbQueryConnection): string[] {
  return db
    .select({ key: pluginKv.key })
    .from(pluginKv)
    .where(
      and(
        eq(pluginKv.pluginId, namespace),
        like(pluginKv.key, "thread:%"),
        sql`json_extract(${pluginKv.value}, '$.mode') = 'read-only'`,
      ),
    )
    .all()
    .map((row) => row.key.slice(7));
}
export function cancelRuntimeRecovery(
  db: DbQueryConnection,
  threadId: string,
  reason: string,
): void {
  db.transaction(
    (tx) => {
      const state = readRecovery(tx, threadId);
      if (!state) return;
      writeRecovery(tx, threadId, {
        ...state,
        mode: "off",
        generation: state.generation + 1,
        decision: reason,
      });
      const host = readRecoveryHost(tx, state.hostId);
      if (
        host.owner === threadId &&
        !state.attempts.some(
          (a) => a.phase === "dispatching" || a.phase === "unknown",
        )
      )
        writeRecoveryHost(tx, state.hostId, { ...host, owner: null });
    },
    { behavior: "immediate" },
  );
}
export function reconcileRecoveryHost(
  db: DbQueryConnection,
  hostId: string,
  sessionId: string | null,
  now: number,
): void {
  db.transaction(
    (tx) => {
      const state = readRecoveryHost(tx, hostId);
      writeRecoveryHost(tx, hostId, {
        ...state,
        sessionId,
        reconciledAt: now,
        connectedAt:
          sessionId !== null && sessionId === state.sessionId
            ? state.connectedAt
            : 0,
      });
    },
    { behavior: "immediate" },
  );
}
export function connectRecoveryHost(
  db: DbQueryConnection,
  hostId: string,
  sessionId: string,
  now: number,
): void {
  db.transaction(
    (tx) => {
      const state = readRecoveryHost(tx, hostId);
      if (state.sessionId === sessionId)
        writeRecoveryHost(tx, hostId, { ...state, connectedAt: now });
    },
    { behavior: "immediate" },
  );
}
export function reserveRecovery(
  db: DbQueryConnection,
  threadId: string,
  expectedGeneration: number,
  now: number,
): RecoveryState | null {
  const state = readRecovery(db, threadId);
  if (
    !state ||
    state.mode !== "read-only" ||
    state.generation !== expectedGeneration
  )
    throw new Error("recovery_generation_changed");
  const host = readRecoveryHost(db, state.hostId);
  if (
    !host.sessionId ||
    host.connectedAt === 0 ||
    now - Math.max(host.reconciledAt, host.connectedAt) < 60_000
  )
    throw new Error("host_not_stable");
  if (host.blockedUntil > now) throw new Error("host_recovery_held");
  if (
    now < state.notBefore ||
    now - state.openedAt >= 30 * 60_000 ||
    state.attempts.length >= 2
  )
    throw new Error("recovery_budget_or_delay");
  if (state.attempts.some((a) => a.requestId === state.expectedRequestId))
    throw new Error("recovery_already_attempted");
  if (state.attempts.some((a) => a.phase !== "dispatched"))
    throw new Error("recovery_inflight_or_unknown");
  const incidents = host.incidents.filter((i) => now - i.at < 300_000);
  if (!incidents.some((i) => i.threadId === threadId))
    incidents.push({ threadId, at: now });
  const blockedUntil =
    new Set(incidents.map((i) => i.threadId)).size >= 3
      ? now + 600_000
      : host.blockedUntil;
  writeRecoveryHost(db, state.hostId, {
    ...host,
    incidents,
    blockedUntil,
    owner: blockedUntil > now ? host.owner : (host.owner ?? threadId),
  });
  if (blockedUntil > now || (host.owner !== null && host.owner !== threadId)) {
    writeRecovery(db, threadId, {
      ...state,
      decision: blockedUntil > now ? "host-circuit-open" : "host-capacity",
    });
    return null;
  }
  const generation = state.generation + 1;
  const next = {
    ...state,
    generation,
    decision: "reserved",
    attempts: [
      ...state.attempts,
      {
        requestId: state.expectedRequestId,
        generation,
        at: now,
        phase: "reserved" as const,
        queueId: null,
      },
    ],
  };
  writeRecovery(db, threadId, next);
  return next;
}

import { and, desc, eq, gt, inArray, sql } from "drizzle-orm";
import {
  events,
  getThread,
  getEnvironment,
  getLastStoredTurnRequestEvent,
  hasQueuedRetryOfTurnRequest,
  getQueuedThreadMessage,
  listQueuedThreadMessages,
  listPendingInteractionsByThread,
  listActiveBackgroundTaskCountsByThreadIds,
  type DbQueryConnection,
} from "@bb/db";
import type { Thread } from "@bb/domain";
import { z } from "zod";
import type { LoggedPendingInteractionWorkSessionDeps } from "../../types.js";
import { ApiError } from "../../errors.js";
import { parseStoredTurnRequestEvent } from "./thread-events.js";
import { retryChain } from "./turn-failed.js";
import { retryFailedTurn } from "./turn-retry.js";
import {
  hasLiveThreadStartInFlight,
  hasLiveThreadStopInFlight,
} from "./thread-lifecycle.js";
import { callHostOnlineRpcForWork } from "../hosts/online-rpc.js";
import {
  cancelRuntimeRecovery,
  listRecoveryThreads,
  readRecovery,
  readRecoveryEpisode,
  readRecoveryHost,
  reserveRecovery,
  writeRecovery,
  writeRecoveryHost,
  type RecoveryState,
} from "./runtime-recovery-store.js";

type Deps = LoggedPendingInteractionWorkSessionDeps;
export interface RuntimeRecoveryMarker {
  generation: number;
  expectedRequestId: string;
}
const interruptionSchema = z.object({ reason: z.string() });
const goalSchema = z.object({
  kind: z.string(),
  payload: z.object({ status: z.string() }).nullable(),
});

function currentRequest(db: DbQueryConnection, threadId: string) {
  const row = getLastStoredTurnRequestEvent(db, threadId);
  return row
    ? { request: parseStoredTurnRequestEvent(row), sequence: row.sequence }
    : null;
}
function interruption(db: DbQueryConnection, threadId: string, after: number) {
  const row = db
    .select()
    .from(events)
    .where(
      and(
        eq(events.threadId, threadId),
        eq(events.type, "system/thread/interrupted"),
        gt(events.sequence, after),
      ),
    )
    .orderBy(desc(events.sequence))
    .limit(1)
    .get();
  return row
    ? { ...interruptionSchema.parse(JSON.parse(row.data)), at: row.createdAt }
    : null;
}
function providerOwnsFailure(
  db: DbQueryConnection,
  threadId: string,
  after: number,
): boolean {
  return (
    db
      .select({ sequence: events.sequence })
      .from(events)
      .where(
        and(
          eq(events.threadId, threadId),
          eq(events.type, "provider/error"),
          gt(events.sequence, after),
        ),
      )
      .limit(1)
      .get() !== undefined
  );
}
function assertReadOnlyEvidence(
  db: DbQueryConnection,
  threadId: string,
  originalRequestId: string,
): void {
  const original = db
    .select({ sequence: events.sequence })
    .from(events)
    .where(
      and(
        eq(events.threadId, threadId),
        inArray(events.type, [
          "client/thread/start",
          "client/turn/requested",
          "client/turn/start",
        ]),
        sql`json_extract(${events.data}, '$.requestId') = ${originalRequestId}`,
      ),
    )
    .limit(1)
    .get();
  if (!original)
    throw new ApiError(
      409,
      "recovery_evidence_missing",
      "Original task evidence is unavailable",
    );
  const rows = db
    .select({ type: events.type, data: events.data })
    .from(events)
    .where(
      and(
        eq(events.threadId, threadId),
        gt(events.sequence, original.sequence),
        inArray(events.type, [
          "item/started",
          "item/completed",
          "provider/unhandled",
        ]),
      ),
    )
    .orderBy(events.sequence)
    .all();
  const allowed = new Set([
    "userMessage",
    "agentMessage",
    "reasoning",
    "plan",
    "planSteps",
    "fileRead",
    "search",
    "webSearch",
    "webFetch",
    "imageView",
    "contextCompaction",
  ]);
  const compacting = new Set<string>();
  for (const row of rows) {
    const data = z
      .object({ item: z.object({ type: z.string(), id: z.string() }) })
      .safeParse(JSON.parse(row.data));
    if (!data.success || !allowed.has(data.data.item.type))
      throw new ApiError(
        409,
        "recovery_external_effect_unverified",
        "Opaque tool or write outcome requires an operator and operation ledger",
      );
    if (data.data.item.type === "contextCompaction") {
      if (row.type === "item/started") compacting.add(data.data.item.id);
      else compacting.delete(data.data.item.id);
    }
  }
  if (compacting.size > 0)
    throw new ApiError(
      409,
      "recovery_compaction_pending",
      "Compaction outcome is not settled",
    );
}
export function configureRuntimeRecovery(
  db: DbQueryConnection,
  thread: Thread,
  mode: "off" | "read-only",
  now = Date.now(),
): void {
  db.transaction(
    (tx) => configureRuntimeRecoveryInTransaction(tx, thread, mode, now),
    { behavior: "immediate" },
  );
}
function configureRuntimeRecoveryInTransaction(
  db: DbQueryConnection,
  thread: Thread,
  mode: "off" | "read-only",
  now: number,
): void {
  if (mode === "off") {
    cancelRuntimeRecovery(db, thread.id, "opt-out");
    return;
  }
  const current = currentRequest(db, thread.id);
  const env = thread.environmentId
    ? getEnvironment(db, thread.environmentId)
    : null;
  if (
    !current ||
    !env ||
    thread.archivedAt !== null ||
    thread.deletedAt !== null
  )
    throw new ApiError(
      409,
      "invalid_request",
      "Recovery requires an existing task and ready environment",
    );
  const previous = readRecovery(db, thread.id);
  const originalRequestId = retryChain(current.request).originalRequestId;
  const historical = readRecoveryEpisode(db, thread.id, originalRequestId);
  if (historical && previous?.originalRequestId !== originalRequestId)
    throw new ApiError(
      409,
      "recovery_episode_exists",
      "This original task already has a durable recovery episode",
    );
  if (previous?.originalRequestId === originalRequestId) {
    if (previous.mode === "off")
      throw new ApiError(
        409,
        "invalid_request",
        "This recovery episode was canceled; start a new task before opting in again",
      );
    return;
  }
  writeRecovery(db, thread.id, {
    mode,
    generation: (previous?.generation ?? 0) + 1,
    originalRequestId,
    expectedRequestId: current.request.requestId,
    hostId: env.hostId,
    openedAt: 0,
    notBefore: now,
    attempts: [],
    decision: "opted-in",
  });
}
function assertEligible(
  deps: Deps,
  db: DbQueryConnection,
  threadId: string,
  marker: RuntimeRecoveryMarker,
  now: number,
): RecoveryState {
  const state = readRecovery(db, threadId);
  const thread = getThread(db, threadId);
  const request = currentRequest(db, threadId);
  if (
    !state ||
    state.mode !== "read-only" ||
    state.generation !== marker.generation ||
    state.expectedRequestId !== marker.expectedRequestId ||
    !thread ||
    thread.status !== "error" ||
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    !request ||
    request.request.requestId !== marker.expectedRequestId ||
    retryChain(request.request).originalRequestId !== state.originalRequestId
  )
    throw new ApiError(
      409,
      "recovery_stale",
      "Recovery task or cancellation generation changed",
    );
  if (
    hasLiveThreadStartInFlight(threadId) ||
    hasLiveThreadStopInFlight(threadId) ||
    deps.pendingInteractions.hasTurnBoundPendingThreadInteraction(threadId)
  )
    throw new ApiError(
      409,
      "recovery_busy",
      "Runtime transition or interaction is pending",
    );
  if (
    listPendingInteractionsByThread(db, {
      threadId,
      statuses: ["pending", "resolving"],
    }).length > 0 ||
    (listActiveBackgroundTaskCountsByThreadIds(db, { threadIds: [threadId] })[0]
      ?.activeBackgroundAgentCount ?? 0) > 0
  )
    throw new ApiError(
      409,
      "recovery_busy",
      "An interaction or background task is still active",
    );
  assertReadOnlyEvidence(db, threadId, state.originalRequestId);
  const host = readRecoveryHost(db, state.hostId);
  if (
    !host.sessionId ||
    deps.hub.getDaemonSessionIdForHost(state.hostId) !== host.sessionId ||
    host.connectedAt === 0 ||
    now - Math.max(host.reconciledAt, host.connectedAt) < 60_000 ||
    host.blockedUntil > now ||
    host.owner !== threadId
  )
    throw new ApiError(
      409,
      "recovery_host_unstable",
      "Host has not reconciled and remained stable",
    );
  if (
    now - state.openedAt >= 1800_000 ||
    now < state.notBefore ||
    state.attempts.length > 2 ||
    retryChain(request.request).attemptNumber >= 3
  )
    throw new ApiError(
      409,
      "recovery_budget",
      "Recovery episode budget exhausted",
    );
  const ownQueueId = state.attempts.find(
    (a) => a.generation === marker.generation,
  )?.queueId;
  if (
    listQueuedThreadMessages(db, threadId).some((row) => row.id !== ownQueueId)
  )
    throw new ApiError(
      409,
      "recovery_queue_changed",
      "Another input or native retry is queued",
    );
  const environment = thread.environmentId
    ? getEnvironment(db, thread.environmentId)
    : null;
  if (
    !environment ||
    environment.status !== "ready" ||
    environment.hostId !== state.hostId
  )
    throw new ApiError(409, "recovery_environment", "Environment changed");
  if (providerOwnsFailure(db, threadId, request.sequence))
    throw new ApiError(
      409,
      "recovery_provider_owned",
      "Provider retry or operator owns this failure",
    );
  const lost = interruption(db, threadId, request.sequence);
  if (lost?.reason !== "host-daemon-restarted")
    throw new ApiError(
      409,
      "recovery_not_runtime_loss",
      "Failure is not confirmed daemon runtime loss",
    );
  const directGoal = db
    .select({ type: events.type, data: events.data })
    .from(events)
    .where(
      and(
        eq(events.threadId, threadId),
        inArray(events.type, ["thread/goal/updated", "thread/goal/cleared"]),
      ),
    )
    .orderBy(desc(events.sequence))
    .limit(1)
    .get();
  if (
    directGoal?.type === "thread/goal/updated" &&
    z.object({ status: z.string() }).parse(JSON.parse(directGoal.data))
      .status !== "active"
  )
    throw new ApiError(
      409,
      "recovery_goal_paused",
      "Goal does not allow continuation",
    );
  const goal = db
    .select({ data: events.data })
    .from(events)
    .where(
      and(
        eq(events.threadId, threadId),
        eq(events.type, "thread/extensionState/updated"),
      ),
    )
    .orderBy(desc(events.sequence))
    .all()
    .find((row) =>
      goalSchema.safeParse(JSON.parse(row.data)).data?.kind.endsWith("/goal"),
    );
  const status = goal
    ? goalSchema.parse(JSON.parse(goal.data)).payload?.status
    : null;
  if (status && status !== "active")
    throw new ApiError(
      409,
      "recovery_goal_paused",
      "Goal does not allow continuation",
    );
  return state;
}

export function runtimeRecoveryQueueGuard(
  deps: Deps,
  threadId: string,
  marker: RuntimeRecoveryMarker,
) {
  return {
    beforeWrite: (db: DbQueryConnection) => {
      assertEligible(deps, db, threadId, marker, Date.now());
    },
    afterWrite: (db: DbQueryConnection, row: { id: string }) => {
      const state = readRecovery(db, threadId)!;
      writeRecovery(db, threadId, {
        ...state,
        decision: "queued",
        attempts: state.attempts.map((a) =>
          a.generation === marker.generation
            ? { ...a, phase: "queued" as const, queueId: row.id }
            : a,
        ),
      });
    },
  };
}

export async function prepareRuntimeRecoveryDispatch(
  deps: Deps,
  threadId: string,
  marker: RuntimeRecoveryMarker,
) {
  const state = assertEligible(deps, deps.db, threadId, marker, Date.now());
  const env = getEnvironment(
    deps.db,
    getThread(deps.db, threadId)!.environmentId!,
  );
  if (!env || env.status !== "ready")
    throw new ApiError(409, "recovery_environment", "Environment is not ready");
  const sessionId = deps.hub.getDaemonSessionIdForHost(state.hostId);
  const result = await callHostOnlineRpcForWork(deps, {
    hostId: state.hostId,
    command: {
      type: "thread.recovery.inspect",
      environmentId: env.id,
      threadId,
    },
    timeoutMs: 5000,
  });
  assertEligible(deps, deps.db, threadId, marker, Date.now());
  if (
    !result.token ||
    sessionId !== deps.hub.getDaemonSessionIdForHost(state.hostId)
  )
    throw new ApiError(
      409,
      "recovery_runtime_alive",
      "Daemon reports a runtime or changed session",
    );
  return {
    token: result.token,
    beforeSend: (dispatchRequestId: string) => {
      const live = readRecovery(deps.db, threadId);
      const thread = getThread(deps.db, threadId);
      const host = readRecoveryHost(deps.db, state.hostId);
      if (
        !live ||
        live.mode === "off" ||
        live.generation !== marker.generation ||
        currentRequest(deps.db, threadId)?.request.requestId !==
          dispatchRequestId ||
        thread?.status !== "active" ||
        thread.archivedAt !== null ||
        thread.deletedAt !== null ||
        hasLiveThreadStopInFlight(threadId) ||
        deps.hub.getDaemonSessionIdForHost(state.hostId) !== sessionId ||
        host.owner !== threadId ||
        host.blockedUntil > Date.now() ||
        host.connectedAt === 0 ||
        Date.now() - Math.max(host.connectedAt, host.reconciledAt) < 60_000 ||
        Date.now() - live.openedAt >= 1800_000
      )
        throw new ApiError(
          409,
          "recovery_stale",
          "Recovery canceled before transport dispatch",
        );
    },
    settled: (success: boolean) => {
      deps.db.transaction(
        (tx) => {
          const live = readRecovery(tx, threadId);
          if (!live || live.generation !== marker.generation) return;
          writeRecovery(tx, threadId, {
            ...live,
            attempts: live.attempts.map((a) =>
              a.generation === marker.generation
                ? {
                    ...a,
                    phase: success
                      ? ("dispatched" as const)
                      : ("unknown" as const),
                  }
                : a,
            ),
            decision: success ? "acknowledged" : "ack-unknown",
          });
          const host = readRecoveryHost(tx, state.hostId);
          if (success && host.owner === threadId)
            writeRecoveryHost(tx, state.hostId, { ...host, owner: null });
        },
        { behavior: "immediate" },
      );
    },
    beforeAppend: (db: DbQueryConnection) => {
      const current = assertEligible(deps, db, threadId, marker, Date.now());
      const attempts = current.attempts.map((a) =>
        a.generation === marker.generation
          ? { ...a, phase: "dispatching" as const }
          : a,
      );
      if (
        !attempts.some(
          (a) =>
            a.generation === marker.generation && a.phase === "dispatching",
        )
      )
        throw new ApiError(
          409,
          "recovery_claim_lost",
          "Recovery claim disappeared",
        );
      writeRecovery(db, threadId, {
        ...current,
        attempts,
        decision: "dispatching",
      });
    },
  };
}

function noteRecoveryDecision(
  db: DbQueryConnection,
  threadId: string,
  generation: number,
  decision: string,
): void {
  db.transaction(
    (tx) => {
      const state = readRecovery(tx, threadId);
      if (
        state?.mode === "read-only" &&
        state.generation === generation &&
        state.decision !== decision
      )
        writeRecovery(tx, threadId, { ...state, decision });
    },
    { behavior: "immediate" },
  );
}

export async function sweepRuntimeRecovery(
  deps: Deps,
  now: number,
): Promise<void> {
  for (const threadId of listRecoveryThreads(deps.db)) {
    let state = readRecovery(deps.db, threadId);
    if (!state || state.mode === "off") continue;
    try {
      const thread = getThread(deps.db, threadId);
      const current = currentRequest(deps.db, threadId);
      if (
        !thread ||
        !current ||
        thread.archivedAt !== null ||
        thread.deletedAt !== null ||
        retryChain(current.request).originalRequestId !==
          state.originalRequestId
      ) {
        cancelRuntimeRecovery(deps.db, threadId, "task-changed");
        continue;
      }
      const last = state.attempts.at(-1);
      if (last && last.requestId === current.request.requestId) {
        if (
          last.phase === "queued" &&
          last.queueId !== null &&
          !getQueuedThreadMessage(deps.db, last.queueId)
        )
          cancelRuntimeRecovery(deps.db, threadId, "queue-canceled");
        continue;
      }
      if (thread.status !== "error") continue;
      const lost = interruption(deps.db, threadId, current.sequence);
      if (lost?.reason !== "host-daemon-restarted") {
        noteRecoveryDecision(
          deps.db,
          threadId,
          state.generation,
          "runtime-loss-not-confirmed",
        );
        continue;
      }
      if (providerOwnsFailure(deps.db, threadId, current.sequence)) {
        noteRecoveryDecision(
          deps.db,
          threadId,
          state.generation,
          "provider-owned",
        );
        continue;
      }
      if (
        hasQueuedRetryOfTurnRequest(deps.db, {
          threadId,
          retryOfTurnRequestId: state.originalRequestId,
        })
      ) {
        noteRecoveryDecision(
          deps.db,
          threadId,
          state.generation,
          "native-retry-queued",
        );
        continue;
      }
      if (
        state.expectedRequestId !== current.request.requestId ||
        state.openedAt === 0
      ) {
        const expectedGeneration = state.generation;
        state = deps.db.transaction(
          (tx) => {
            const live = readRecovery(tx, threadId);
            if (
              !live ||
              live.mode === "off" ||
              live.generation !== expectedGeneration
            )
              throw new ApiError(
                409,
                "recovery_stale",
                "Recovery changed before admission",
              );
            const next = {
              ...live,
              expectedRequestId: current.request.requestId,
              openedAt: live.openedAt || lost.at,
              notBefore:
                lost.at +
                (live.attempts.length === 0 ? 60_000 : 180_000) +
                Math.floor(Math.random() * 30_001),
            };
            writeRecovery(tx, threadId, next);
            return next;
          },
          { behavior: "immediate" },
        );
      }
      const reserved = deps.db.transaction(
        (tx) => reserveRecovery(tx, threadId, state!.generation, now),
        { behavior: "immediate" },
      );
      if (!reserved) continue;
      const recovery = {
        generation: reserved.generation,
        expectedRequestId: reserved.expectedRequestId,
      };
      assertEligible(deps, deps.db, threadId, recovery, now);
      await prepareRuntimeRecoveryDispatch(deps, threadId, recovery);
      const outcome = await retryFailedTurn(deps, {
        thread,
        request: {
          turnRequestId: current.request.requestId,
          sendAt: null,
          reason: "Runtime recovery (read-only opt-in)",
        },
        recovery,
      });
      deps.db.transaction(
        (tx) => {
          const live = readRecovery(tx, threadId);
          if (!live || live.generation !== reserved.generation) return;
          writeRecovery(tx, threadId, {
            ...live,
            attempts: live.attempts.map((a) =>
              a.generation === reserved.generation && a.phase === "reserved"
                ? {
                    ...a,
                    phase:
                      outcome.delivery === "queued"
                        ? ("queued" as const)
                        : ("dispatched" as const),
                    queueId:
                      outcome.delivery === "queued"
                        ? outcome.queuedMessageId
                        : null,
                  }
                : a,
            ),
            decision: outcome.delivery,
          });
        },
        { behavior: "immediate" },
      );
    } catch (error) {
      deps.db.transaction(
        (tx) => {
          const live = readRecovery(tx, threadId);
          if (live && live.mode !== "off")
            writeRecovery(tx, threadId, {
              ...live,
              decision:
                error instanceof ApiError
                  ? error.body.code
                  : error instanceof Error
                    ? error.message
                    : "unknown",
            });
        },
        { behavior: "immediate" },
      );
      deps.logger.debug({ threadId, err: error }, "Runtime recovery declined");
    }
  }
}

export { cancelRuntimeRecoveryForThread } from "./runtime-recovery-cancel.js";

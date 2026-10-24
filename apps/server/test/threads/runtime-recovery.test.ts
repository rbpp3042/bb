import { handleHostSessionOpened } from "../../src/internal/session-owner-side-effects.js";
import { afterEach, describe, expect, it } from "vitest";
import { createDeferredPromise } from "@bb/test-helpers";
import type { PluginHookName } from "@get-bb/plugin-sdk";
import {
  setPluginHookProvider,
  invokePluginInline,
  type PluginHookRegistration,
} from "../../src/services/plugins/plugin-hook-registry.js";
afterEach(() => setPluginHookProvider(undefined));
import {
  getLatestThreadSequence,
  getLastStoredTurnRequestEvent,
  getThread,
  listEvents,
  listQueuedThreadMessages,
} from "@bb/db";
import {
  configureRuntimeRecovery,
  prepareRuntimeRecoveryDispatch,
  runtimeRecoveryQueueGuard,
  sweepRuntimeRecovery,
} from "../../src/services/threads/runtime-recovery.js";
import { cancelRuntimeRecoveryForThread } from "../../src/services/threads/runtime-recovery-cancel.js";
import {
  connectRecoveryHost,
  readRecovery,
  readRecoveryHost,
  reconcileRecoveryHost,
  reserveRecovery,
  writeRecovery,
} from "../../src/services/threads/runtime-recovery-store.js";
import { recordQueuedMessageWait } from "../../src/services/threads/queue-waits.js";
import { applyLoggedThreadLifecycleEvent } from "../../src/services/threads/lifecycle-outcome.js";
import { runQueuedMessageDispatch } from "../../src/services/threads/queued-message-dispatch.js";
import { requestThreadStopForCurrentState } from "../../src/services/threads/thread-lifecycle.js";
import { retryFailedTurn } from "../../src/services/threads/turn-retry.js";
import { toThreadQueuedMessage } from "../../src/services/threads/thread-queued-messages.js";
import {
  seedEnvironment,
  seedEvent,
  seedHostSession,
  seedProjectWithSource,
  seedQueuedMessage,
  seedThread,
  seedThreadRuntimeState,
  seedSession,
} from "../helpers/seed.js";
import {
  listQueuedThreadCommands,
  reportQueuedCommandSuccess,
  waitForQueuedCommand,
} from "../helpers/commands.js";
import { withTestHarness, type TestAppHarness } from "../helpers/test-app.js";

function fixture(
  h: TestAppHarness,
  reason:
    | "host-daemon-restarted"
    | "manual-stop"
    | "host-removed"
    | "provider-turn-idle" = "host-daemon-restarted",
) {
  const { host, session } = seedHostSession(h.deps);
  const { project } = seedProjectWithSource(h.deps, { hostId: host.id });
  const environment = seedEnvironment(h.deps, {
    hostId: host.id,
    projectId: project.id,
    status: "ready",
  });
  const thread = seedThread(h.deps, {
    projectId: project.id,
    environmentId: environment.id,
    status: "active",
  });
  seedThreadRuntimeState(h.deps, {
    environmentId: environment.id,
    threadId: thread.id,
    providerThreadId: "provider-recovery",
    inputText: "Inspect this repository",
  });
  const requestId = JSON.parse(
    getLastStoredTurnRequestEvent(h.db, thread.id)!.data,
  ).requestId;
  const now = Date.now();
  seedEvent(h.deps, {
    threadId: thread.id,
    environmentId: environment.id,
    sequence: getLatestThreadSequence(h.db, { threadId: thread.id }) + 1,
    type: "system/thread/interrupted",
    scope: { kind: "thread" },
    data: { reason },
    createdAt: now - 120_000,
  });
  applyLoggedThreadLifecycleEvent(h.deps, {
    threadId: thread.id,
    event: { type: "run.failed" },
  });
  const failed = getThread(h.db, thread.id)!;
  configureRuntimeRecovery(h.db, failed, "read-only", now - 120_000);
  reconcileRecoveryHost(h.db, host.id, session.id, now - 120_000);
  connectRecoveryHost(h.db, host.id, session.id, now - 120_000);
  return { thread: failed, host, session, environment, requestId, now };
}
async function probe(
  h: TestAppHarness,
  threadId: string,
  token: string | null,
) {
  const cmd = await waitForQueuedCommand(
    h,
    ({ command }) =>
      command.type === "thread.recovery.inspect" &&
      command.threadId === threadId,
  );
  await reportQueuedCommandSuccess(h, cmd, { token });
}
function pendingDispatch(h: TestAppHarness, id: string) {
  return [
    ...listQueuedThreadCommands(h, "turn.submit", id),
    ...listQueuedThreadCommands(h, "thread.start", id),
  ];
}

function queueRecovery(h: TestAppHarness, f: ReturnType<typeof fixture>) {
  const state = readRecovery(h.db, f.thread.id)!;
  writeRecovery(h.db, f.thread.id, {
    ...state,
    openedAt: f.now - 120_000,
    notBefore: f.now - 1,
  });
  const admitted = h.db.transaction(
    (tx) => reserveRecovery(tx, f.thread.id, state.generation, Date.now()),
    { behavior: "immediate" },
  )!;
  const marker = {
    generation: admitted.generation,
    expectedRequestId: f.requestId,
  };
  const args = {
    thread: f.thread,
    message: {
      input: [
        { type: "text" as const, text: "Please continue.", mentions: [] },
      ],
      execution: {
        source: "client/turn/requested" as const,
        model: "gpt-5",
        reasoningLevel: "medium",
        permissionMode: "full" as const,
        serviceTier: "default",
      },
      senderThreadId: null,
      origin: null,
      originPluginId: null,
      requestedBy: null,
      payload: {
        kind: "retry" as const,
        retryOfTurnRequestId: f.requestId,
        attempt: 2,
        reason: "Read-only recovery",
        runtimeRecovery: marker,
      },
      systemNotice: null,
    },
    waitingOn: { kind: "time" as const },
    sendAt: Date.now() + 10_000,
    claimed: null,
    ...runtimeRecoveryQueueGuard(h.deps, f.thread.id, marker),
  };
  const queued = recordQueuedMessageWait(h.deps, args)!;
  return { queued, args, marker };
}

describe("runtime recovery through native core dispatch", () => {
  it("never inspects or dispatches without opt-in", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      configureRuntimeRecovery(h.db, f.thread, "off");
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(
        listQueuedThreadCommands(h, "thread.recovery.inspect", f.thread.id),
      ).toHaveLength(0);
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
    }));
  it.each(["host-removed", "provider-turn-idle", "manual-stop"] as const)(
    "does not infer runtime loss from %s",
    async (reason) =>
      withTestHarness(async (h) => {
        const f = fixture(h, reason);
        await sweepRuntimeRecovery(h.deps, Date.now());
        expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(0);
        expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      }),
  );
  it("declines a live daemon runtime without a second dispatch", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      const sweep = sweepRuntimeRecovery(h.deps, Date.now());
      await probe(h, f.thread.id, null);
      await sweep;
      expect(readRecovery(h.db, f.thread.id)!.decision).toBe(
        "recovery_runtime_alive",
      );
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(1);
    }));
  it.each([false, true])(
    "uses native retry acceptance semantics (accepted=%s)",
    async (accepted) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        if (accepted)
          seedEvent(h.deps, {
            threadId: f.thread.id,
            environmentId: f.environment.id,
            sequence:
              getLatestThreadSequence(h.db, { threadId: f.thread.id }) + 1,
            type: "turn/input/accepted",
            scope: { kind: "turn", turnId: "turn-recovery" },
            data: {
              providerThreadId: "provider-recovery",
              clientRequestId: f.requestId,
            },
          });
        const sweep = sweepRuntimeRecovery(h.deps, Date.now());
        await probe(h, f.thread.id, "probe-1");
        await probe(h, f.thread.id, "probe-2");
        await sweep;
        const dispatch = await waitForQueuedCommand(
          h,
          ({ command }) =>
            (command.type === "turn.submit" ||
              command.type === "thread.start") &&
            command.threadId === f.thread.id,
        );
        expect(dispatch.command).toHaveProperty("recoveryToken", "probe-2");
        const request = JSON.parse(
          getLastStoredTurnRequestEvent(h.db, f.thread.id)!.data,
        );
        expect(request.retryOfRequestId).toBe(f.requestId);
        expect(
          request.input.map((i: { text?: string }) => i.text).join(" "),
        ).toContain(accepted ? "Please continue." : "Inspect this repository");
        await reportQueuedCommandSuccess(
          h,
          dispatch,
          dispatch.command.type === "turn.submit"
            ? { trace: { spans: [] } }
            : { providerThreadId: "provider-recovery" },
        );
        await new Promise((resolve) => setTimeout(resolve, 10));
        expect(readRecovery(h.db, f.thread.id)!.attempts[0]!.phase).toBe(
          "dispatched",
        );
        expect(readRecoveryHost(h.db, f.host.id).owner).toBeNull();
      }),
  );
  it.each([
    "manual-stop",
    "new-input",
    "queue-canceled",
    "goal-paused",
    "approval-declined",
    "archived",
    "deleted",
  ])("fences %s while the daemon probe is pending", async (reason) =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      const sweep = sweepRuntimeRecovery(h.deps, Date.now());
      const inspection = await waitForQueuedCommand(
        h,
        ({ command }) => command.type === "thread.recovery.inspect",
      );
      cancelRuntimeRecoveryForThread(h.deps, f.thread.id, reason);
      await reportQueuedCommandSuccess(h, inspection, { token: "stale" });
      await sweep;
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      expect(readRecovery(h.db, f.thread.id)!.mode).toBe("off");
    }),
  );
  it("serializes two coordinator workers durably", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      const first = sweepRuntimeRecovery(h.deps, Date.now());
      await waitForQueuedCommand(
        h,
        ({ command }) => command.type === "thread.recovery.inspect",
      );
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(1);
      expect(
        listQueuedThreadCommands(h, "thread.recovery.inspect", f.thread.id),
      ).toHaveLength(1);
      await probe(h, f.thread.id, null);
      await first;
    }));
  it("leaves existing user input and native queued retries alone", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      seedQueuedMessage(h.deps, {
        threadId: f.thread.id,
        content: [{ type: "text", text: "new input", mentions: [] }],
      });
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      expect(listQueuedThreadMessages(h.db, f.thread.id)).toHaveLength(1);
    }));
  it("atomically persists the recovery marker and queue ownership, then fences a late enqueue", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      const { queued, args, marker } = queueRecovery(h, f);
      expect(readRecovery(h.db, f.thread.id)!.attempts[0]!.queueId).toBe(
        queued.id,
      );
      expect(
        toThreadQueuedMessage(listQueuedThreadMessages(h.db, f.thread.id)[0]!)
          .payload,
      ).toMatchObject({ runtimeRecovery: marker });
      cancelRuntimeRecoveryForThread(h.deps, f.thread.id, "manual-stop");
      expect(() => recordQueuedMessageWait(h.deps, args)).toThrow();
      expect(listQueuedThreadMessages(h.db, f.thread.id)).toHaveLength(0);
    }));
  it.each([false, true])(
    "rechecks the daemon after durable dequeue (cancel=%s)",
    async (cancel) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        queueRecovery(h, f);
        const drain = runQueuedMessageDispatch(h.deps, {
          kind: "time-reached",
          now: Date.now() + 20_000,
        });
        const inspection = await waitForQueuedCommand(
          h,
          ({ command }) => command.type === "thread.recovery.inspect",
        );
        if (cancel)
          cancelRuntimeRecoveryForThread(h.deps, f.thread.id, "manual-stop");
        await reportQueuedCommandSuccess(h, inspection, {
          token: "dequeue-probe",
        });
        await drain;
        if (cancel) expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
        else {
          const dispatch = await waitForQueuedCommand(
            h,
            ({ command }) =>
              (command.type === "turn.submit" ||
                command.type === "thread.start") &&
              command.threadId === f.thread.id,
          );
          expect(dispatch.command).toHaveProperty(
            "recoveryToken",
            "dequeue-probe",
          );
          expect(listQueuedThreadMessages(h.db, f.thread.id)).toHaveLength(0);
        }
      }),
  );
  it("keeps the native retry queue as its sole retry owner", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      await retryFailedTurn(h.deps, {
        thread: f.thread,
        request: {
          turnRequestId: f.requestId,
          sendAt: Date.now() + 60_000,
          reason: "native provider retry",
        },
      });
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(0);
      expect(listQueuedThreadMessages(h.db, f.thread.id)).toHaveLength(1);
    }));
  it("the actual manual Stop entrypoint cancels a pending recovery probe", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      const sweep = sweepRuntimeRecovery(h.deps, Date.now());
      const inspection = await waitForQueuedCommand(
        h,
        ({ command }) => command.type === "thread.recovery.inspect",
      );
      requestThreadStopForCurrentState(h.deps, f.thread, f.environment);
      await reportQueuedCommandSuccess(h, inspection, {
        token: "late-stop-token",
      });
      await sweep;
      expect(readRecovery(h.db, f.thread.id)!.mode).toBe("off");
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
    }));

  it.each(["unauthorized", "rate-limit", "overloaded", "unknown"] as const)(
    "leaves provider %s failure and native willRetry to their owner",
    async (category) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        seedEvent(h.deps, {
          threadId: f.thread.id,
          environmentId: f.environment.id,
          sequence:
            getLatestThreadSequence(h.db, { threadId: f.thread.id }) + 1,
          type: "provider/error",
          scope: { kind: "thread" },
          data: {
            providerThreadId: "provider-recovery",
            message: "provider failure",
            willRetry: true,
            errorInfo: { category, providerCode: null, httpStatusCode: null },
          },
        });
        await sweepRuntimeRecovery(h.deps, Date.now());
        expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(0);
        expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      }),
  );
  it.each(["started", "completed"] as const)(
    "requires an operator for an opaque external command, even when %s",
    async (phase) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        seedEvent(h.deps, {
          threadId: f.thread.id,
          environmentId: f.environment.id,
          sequence:
            getLatestThreadSequence(h.db, { threadId: f.thread.id }) + 1,
          type: phase === "started" ? "item/started" : "item/completed",
          scope: { kind: "turn", turnId: "turn-effect" },
          data: {
            providerThreadId: "provider-recovery",
            item: {
              type: "commandExecution",
              id: "effect-op",
              command: "fake receiver write --operation-id op-1",
              cwd: "/tmp",
              status: phase === "started" ? "pending" : "completed",
              approvalStatus: null,
            },
          },
        });
        await sweepRuntimeRecovery(h.deps, Date.now());
        expect(readRecovery(h.db, f.thread.id)!.decision).toBe(
          "recovery_external_effect_unverified",
        );
        expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
      }),
  );
  it("refuses unresolved compaction", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      seedEvent(h.deps, {
        threadId: f.thread.id,
        environmentId: f.environment.id,
        sequence: getLatestThreadSequence(h.db, { threadId: f.thread.id }) + 1,
        type: "item/started",
        scope: { kind: "turn", turnId: "turn-compaction" },
        data: {
          providerThreadId: "provider-recovery",
          item: { type: "contextCompaction", id: "compact-1" },
        },
      });
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(readRecovery(h.db, f.thread.id)!.decision).toBe(
        "recovery_compaction_pending",
      );
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
    }));

  it("refuses a paused durable goal", async () =>
    withTestHarness(async (h) => {
      const f = fixture(h);
      seedEvent(h.deps, {
        threadId: f.thread.id,
        environmentId: f.environment.id,
        sequence: getLatestThreadSequence(h.db, { threadId: f.thread.id }) + 1,
        type: "thread/goal/updated",
        scope: { kind: "thread" },
        data: {
          providerThreadId: "provider-recovery",
          objective: "read repository",
          status: "paused",
          tokenBudget: null,
          tokensUsed: 0,
          timeUsedSeconds: 0,
        },
      });
      await sweepRuntimeRecovery(h.deps, Date.now());
      expect(readRecovery(h.db, f.thread.id)!.decision).toBe(
        "recovery_goal_paused",
      );
      expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
    }));
  it.each([false, true])(
    "waits for reconcile after session replacement (daemon restarted=%s)",
    async (restarted) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        applyLoggedThreadLifecycleEvent(h.deps, {
          threadId: f.thread.id,
          event: { type: "run.started" },
        });
        const replacement = seedSession(h.deps, f.host.id, {
          instanceId: restarted ? "instance-after-crash" : f.session.instanceId,
        });
        await handleHostSessionOpened(h.deps, {
          hostId: f.host.id,
          previousSession: f.session,
          openedSession: replacement,
          activeThreads: restarted ? [] : [{ threadId: f.thread.id }],
          undeliveredEventThreadIds: [],
        });
        await sweepRuntimeRecovery(h.deps, Date.now());
        expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
        expect(readRecoveryHost(h.db, f.host.id)).toMatchObject({
          sessionId: replacement.id,
          connectedAt: 0,
        });
        if (restarted)
          expect(
            listEvents(h.db, { threadId: f.thread.id })
              .filter((row) => row.type === "system/thread/interrupted")
              .at(-1)?.data,
          ).toContain("host-daemon-restarted");
        else expect(getThread(h.db, f.thread.id)!.status).toBe("active");
      }),
  );

  it.each([false, true])(
    "preserves concurrency wait and fences cancellation after an awaited hook (cancel=%s)",
    async (cancel) =>
      withTestHarness(async (h) => {
        const f = fixture(h);
        const entered = createDeferredPromise<void>(),
          release = createDeferredPromise<void>();
        const registry: { [K in PluginHookName]: PluginHookRegistration<K>[] } =
          {
            "message.dispatch": [
              {
                pluginId: "concurrency-limit",
                handler: async () => {
                  entered.resolve();
                  await release.promise;
                  return { action: "wait", reason: "At capacity" };
                },
              },
            ],
          };
        setPluginHookProvider({
          listHooks: (hook) => registry[hook],
          invokeHook: (_id, _label, run) => invokePluginInline(run),
          decisionTimeoutMs: 10_000,
        });
        const sweep = sweepRuntimeRecovery(h.deps, Date.now());
        await probe(h, f.thread.id, "before-plugin-wait");
        await entered.promise;
        if (cancel)
          cancelRuntimeRecoveryForThread(h.deps, f.thread.id, "manual-stop");
        release.resolve();
        await sweep;
        const rows = listQueuedThreadMessages(h.db, f.thread.id);
        expect(rows).toHaveLength(cancel ? 0 : 1);
        expect(pendingDispatch(h, f.thread.id)).toHaveLength(0);
        if (!cancel) {
          expect(toThreadQueuedMessage(rows[0]!).waitingOn).toMatchObject({
            kind: "plugin",
            pluginId: "concurrency-limit",
          });
          await sweepRuntimeRecovery(h.deps, Date.now());
          expect(readRecovery(h.db, f.thread.id)!.attempts).toHaveLength(1);
        }
      }),
  );
});

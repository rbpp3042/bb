import {
  getThread,
  getQueuedThreadMessage,
  deleteQueuedThreadMessage,
} from "@bb/db";
import type { WorkSessionDeps } from "../../types.js";
import { callHostOnlineRpcForWork } from "../hosts/online-rpc.js";
import {
  cancelRuntimeRecovery,
  readRecovery,
  readRecoveryHost,
  writeRecoveryHost,
} from "./runtime-recovery-store.js";

export function cancelRuntimeRecoveryForThread(
  deps: WorkSessionDeps,
  threadId: string,
  reason: string,
): void {
  const state = readRecovery(deps.db, threadId);
  if (!state || state.mode === "off") return;
  deps.db.transaction((tx) => cancelRuntimeRecovery(tx, threadId, reason), {
    behavior: "immediate",
  });
  for (const attempt of state.attempts) {
    if (
      attempt.queueId !== null &&
      getQueuedThreadMessage(deps.db, attempt.queueId)?.threadId === threadId
    )
      deleteQueuedThreadMessage(deps.db, deps.hub, attempt.queueId);
  }
  const thread = getThread(deps.db, threadId);
  if (!thread?.environmentId || !deps.hub.hasDaemonForHost(state.hostId))
    return;
  void callHostOnlineRpcForWork(deps, {
    hostId: state.hostId,
    command: {
      type: "thread.recovery.cancel",
      threadId,
      environmentId: thread.environmentId,
    },
    timeoutMs: 5000,
  })
    .then(() =>
      deps.db.transaction(
        (tx) => {
          const host = readRecoveryHost(tx, state.hostId);
          const current = readRecovery(tx, threadId);
          if (
            !state.attempts.some(
              (a) => a.phase === "dispatching" || a.phase === "unknown",
            ) &&
            host.owner === threadId &&
            current?.mode === "off" &&
            current.generation === state.generation + 1
          )
            writeRecoveryHost(tx, state.hostId, { ...host, owner: null });
        },
        { behavior: "immediate" },
      ),
    )
    .catch((error) =>
      deps.logger.debug(
        { threadId, err: error },
        "Recovery cancellation awaits reconcile",
      ),
    );
}
